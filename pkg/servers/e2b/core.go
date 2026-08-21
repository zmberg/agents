/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2b

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/openkruise/agents/pkg/agent-runtime/storages"
	"github.com/openkruise/agents/pkg/cache"
	sandboxmanager "github.com/openkruise/agents/pkg/sandbox-manager"
	"github.com/openkruise/agents/pkg/sandbox-manager/config"
	"github.com/openkruise/agents/pkg/sandbox-manager/consts"
	"github.com/openkruise/agents/pkg/sandbox-manager/logs"
	"github.com/openkruise/agents/pkg/servers/e2b/adapters"
	"github.com/openkruise/agents/pkg/servers/e2b/keys"
	agentutilruntime "github.com/openkruise/agents/pkg/utils/runtime"
)

// Controller handles sandbox-related operations
type Controller struct {
	// E2B API surface
	maxTimeout            int
	minResumeTimeoutValue int
	domain                string
	keyCfg                *keys.Config

	// mgrOpts is handed to the sandbox-manager builder unchanged. It also carries
	// the system namespace the API handlers fall back to, so it is the single
	// place a manager-level knob has to be threaded through.
	mgrOpts config.SandboxManagerOptions
	// runtimeTLSBundle is the client TLS bundle for reaching TLS-capable
	// agent-runtimes; nil disables runtime TLS for this manager, so every
	// sandbox is served over the legacy plaintext paths.
	runtimeTLSBundle *agentutilruntime.TLSBundle

	// fields
	mux             *http.ServeMux
	server          *http.Server
	stop            chan os.Signal
	cache           cache.Provider
	storageRegistry storages.VolumeMountProviderRegistry
	adapter         *adapters.E2BAdapter
	manager         *sandboxmanager.SandboxManager
	keys            keys.KeyStorage

	// substrate holds the Substrate backend configuration. It is nil unless the
	// substrate backend is enabled, in which case buildTemplate routes are
	// registered and ActorTemplates are created through substrateClient.
	substrate       *SubstrateConfig
	substrateClient client.Client
	// namespaceReader resolves team namespaces. It is the informer-backed client
	// when the backend has a cache, and a direct API reader otherwise, so the
	// team-namespace check works on every backend.
	namespaceReader client.Reader
	// keyStorageMgr is a minimal controller-runtime manager created only when
	// the substrate backend is in use (which has no informer cache of its own)
	// to provide client, api-reader and cache for the secret key storage.
	keyStorageMgr ctrl.Manager
}

// SubstrateConfig carries the deployment-level inputs the E2B template build
// API needs to materialize an ActorTemplate, which the E2B protocol itself does
// not supply.
type SubstrateConfig struct {
	// Address is the Substrate control gRPC address; a non-empty value enables
	// the substrate backend.
	Address string
	// CAFile verifies the Substrate server certificate for TLS addresses.
	CAFile string
	// PauseImage is the pinned pause container image for generated ActorTemplates.
	PauseImage string
	// SnapshotsLocationBase is the root snapshot location; the per-template
	// location is this joined with the team namespace.
	SnapshotsLocationBase string
	// SandboxClass selects the runtime family for generated ActorTemplates.
	SandboxClass string
	// DefaultHibernateMode is the fallback hibernate mode for claims.
	DefaultHibernateMode string
}

// Enabled reports whether the substrate backend is configured.
func (c *SubstrateConfig) Enabled() bool {
	return c != nil && c.Address != ""
}

// ControllerOptions carries everything NewController needs. Passing a struct
// instead of a long positional parameter list keeps every value named at the
// call site, so adding a knob cannot silently shift an argument.
type ControllerOptions struct {
	// Domain is the static E2B domain. When empty the domain is resolved per
	// request from the HTTP Host header.
	Domain string
	// Port is the port the E2B HTTP server listens on.
	Port int
	// MaxTimeout is the E2B maximum sandbox timeout in seconds.
	MaxTimeout int
	// MinResumeTimeout is the floor, in seconds, applied to the timeout carried
	// by the E2B connect API.
	MinResumeTimeout int
	// KeyConfig configures API key storage. Nil disables E2B authentication.
	KeyConfig *keys.Config

	// Manager is passed to the sandbox-manager builder unchanged.
	Manager config.SandboxManagerOptions
	// RuntimeTLSBundle is the client TLS bundle used to reach TLS-capable
	// agent-runtimes during claim and clone post-processing. Nil keeps every
	// runtime call on the legacy plaintext paths.
	RuntimeTLSBundle *agentutilruntime.TLSBundle
	// Substrate enables and configures the Substrate backend. Nil or an empty
	// Address keeps the default Sandbox CR backend.
	Substrate *SubstrateConfig
}

// NewController creates a new E2B Controller from opts.
func NewController(opts ControllerOptions) *Controller {
	sc := &Controller{
		mux:                   http.NewServeMux(),
		domain:                opts.Domain,
		adapter:               adapters.DefaultAdapterFactory(opts.Port),
		maxTimeout:            opts.MaxTimeout,
		minResumeTimeoutValue: opts.MinResumeTimeout,
		keyCfg:                opts.KeyConfig,
		mgrOpts:               opts.Manager,
		runtimeTLSBundle:      opts.RuntimeTLSBundle,
		// keyStorageMgr is populated lazily in Init when the substrate backend is active.
		substrate: opts.Substrate,
	}

	sc.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", opts.Port),
		Handler:           sc.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return sc
}

func (sc *Controller) Init() error {
	ctx := logs.NewContext()
	log := klog.FromContext(ctx)
	log.Info("init controller")

	builder := sandboxmanager.NewSandboxManagerBuilder(sc.mgrOpts).
		WithMemberlistPeers().
		WithRequestAdapter(sc.adapter).
		WithRuntimeTLSBundle(sc.runtimeTLSBundle)

	if sc.substrate.Enabled() {
		log.Info("using substrate backend", "address", sc.substrate.Address)
		builder = builder.WithSubstrateInfra(sandboxmanager.SubstrateOptions{
			Address:              sc.substrate.Address,
			CAFile:               sc.substrate.CAFile,
			DefaultHibernateMode: sc.substrate.DefaultHibernateMode,
		})
	} else {
		builder = builder.WithSandboxInfra()
	}

	sandboxManager, err := builder.Build()
	if err != nil {
		return err
	}

	sc.manager = sandboxManager
	sc.cache = sandboxManager.GetInfra().GetCache()
	sc.storageRegistry = storages.NewStorageProvider()

	if err := sc.initNamespaceReader(); err != nil {
		return err
	}

	// The substrate backend keeps no informer cache, so buildTemplate handlers
	// need their own client to create and read ActorTemplates.
	if sc.substrate.Enabled() {
		templateClient, err := client.New(sc.mgrOpts.RestConfig, client.Options{Scheme: substrateScheme})
		if err != nil {
			return fmt.Errorf("create substrate ActorTemplate client: %w", err)
		}
		sc.substrateClient = templateClient
	}

	// The substrate backend has no informer cache (GetCache returns nil). The
	// secret key storage requires a controller-runtime client, api-reader, and
	// cache to watch for API-key changes. Build a lightweight manager for this
	// purpose, scoped only to the system namespace.
	if sc.keyCfg != nil && sc.cache == nil && sc.mgrOpts.RestConfig != nil {
		keyScheme := apimachineryruntime.NewScheme()
		utilruntime.Must(clientgoscheme.AddToScheme(keyScheme))
		keyMgr, err := ctrl.NewManager(sc.mgrOpts.RestConfig, ctrl.Options{
			Scheme:                 keyScheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			LeaderElection:         false,
		})
		if err != nil {
			return fmt.Errorf("create key-storage manager for substrate backend: %w", err)
		}
		sc.keyCfg.Client = keyMgr.GetClient()
		sc.keyCfg.APIReader = keyMgr.GetAPIReader()
		sc.keyCfg.Cache = keyMgr.GetCache()
		sc.keyStorageMgr = keyMgr
	}

	sc.registerRoutes()

	if err := sc.initKeyStorage(ctx); err != nil {
		return err
	}

	// Initialize quota through the sandbox-manager, which owns the runtime lifecycle.
	if sc.keys != nil {
		log.Info("will init quota management with quota options")
		if err := sc.manager.InitQuota(ctx, sc.mgrOpts.Quota, keys.NewQuotaSubjectLister(sc.keys)); err != nil {
			return err
		}
	} else {
		log.Info("api-key quota is unenforced because E2B auth is disabled")
		if err := sc.manager.InitQuota(ctx, config.QuotaOptions{}, nil); err != nil {
			return err
		}
	}
	return nil
}

// initNamespaceReader resolves the reader used to confirm that a team's
// Kubernetes namespace exists.
//
// Backends that expose an informer cache reuse it. The substrate backend has no
// cache at all (Infra.GetCache returns nil), so it falls back to a direct API
// reader. A direct Get is acceptable here: the only caller validates a team
// namespace while creating an API key, a rare administrative operation, and the
// alternative would be a cluster-wide Namespace watch maintained solely for it.
func (sc *Controller) initNamespaceReader() error {
	if sc.cache != nil {
		sc.namespaceReader = sc.cache.GetClient()
		return nil
	}
	if sc.mgrOpts.RestConfig == nil {
		// Unit tests build a Controller without a rest config; leave the reader
		// nil and let validateTeamNamespace report it instead of panicking.
		return nil
	}
	namespaceScheme := apimachineryruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(namespaceScheme))
	reader, err := client.New(sc.mgrOpts.RestConfig, client.Options{Scheme: namespaceScheme})
	if err != nil {
		return fmt.Errorf("create namespace reader: %w", err)
	}
	sc.namespaceReader = reader
	return nil
}

func (sc *Controller) initKeyStorage(ctx context.Context) error {
	// Initialize key storage if key config is provided
	if sc.keyCfg != nil {
		var err error
		if sc.cache != nil {
			sc.keyCfg.Client = sc.cache.GetClient()
			sc.keyCfg.APIReader = sc.cache.GetAPIReader()
			sc.keyCfg.Cache = sc.cache.GetCache()
		}
		if sc.keys, err = keys.NewKeyStorage(*sc.keyCfg); err != nil {
			return err
		}
		if err = sc.keys.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (sc *Controller) Run() (context.Context, error) {
	if sc.stop != nil {
		return nil, errors.New("controller already started")
	}
	ctx, cancel := context.WithCancel(logs.NewContext())
	// Channel to listen for interrupt signal
	sc.stop = make(chan os.Signal, 1)
	signal.Notify(sc.stop, syscall.SIGINT, syscall.SIGTERM)
	if err := sc.manager.Run(ctx); err != nil {
		klog.Fatalf("Sandbox manager failed to start: %v", err)
	}

	// When using the substrate backend, start the key-storage manager and wait
	// for its cache to sync before the key store begins watching events.
	if sc.keyStorageMgr != nil {
		go func() {
			if err := sc.keyStorageMgr.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				klog.ErrorS(err, "key-storage manager exited")
			}
		}()
		if !sc.keyStorageMgr.GetCache().WaitForCacheSync(ctx) {
			return nil, fmt.Errorf("key-storage cache failed to sync")
		}
	}

	// Run HTTP server in a goroutine
	go func() {
		klog.InfoS("Starting Server", "address", sc.server.Addr)
		if err := sc.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("HTTP server failed to start: %v", err)
		}
	}()

	// stopper
	go func() {
		<-sc.stop
		shutdownCtx, shutdownCancel := context.WithTimeout(logs.NewContext("action", "shutdown"), consts.ShutdownTimeout)
		defer shutdownCancel()
		sc.shutdown(shutdownCtx, cancel)
	}()

	if sc.keys != nil {
		sc.keys.Run()
	}
	return ctx, nil
}

func (sc *Controller) shutdown(ctx context.Context, cancel context.CancelFunc) {
	log := klog.FromContext(ctx)
	log.Info("Shutting down server...")
	defer cancel()

	if sc.server != nil {
		if err := sc.server.Shutdown(ctx); err != nil {
			klog.ErrorS(err, "HTTP server forced to shutdown")
		}
	}
	if sc.manager != nil {
		sc.manager.Stop(ctx)
	}
	if sc.keys != nil {
		sc.keys.Stop()
	}
	klog.InfoS("Server exited")
}
