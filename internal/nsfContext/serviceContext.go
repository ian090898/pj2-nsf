package nsfContext

import (
	"encoding/json"
	"fmt"
	"sync"

	"nsf/internal/db"
	"nsf/logger"
)

// serviceProvider describes an NF that provides a service.
type serviceProvider struct {
	NfName     string `json:"nf_name"`
	SbiAddress string `json:"sbi_address"`
	SbiPort    int    `json:"sbi_port"`
}

type serviceContextIE struct {
	db.DbIf

	*logger.NsfLogger
}

type serviceContext struct {
	db.DbIf

	*logger.NsfLogger

	mu sync.RWMutex
}

func newServiceContext(ie *serviceContextIE) *serviceContext {
	return &serviceContext{
		DbIf:      ie.DbIf,
		NsfLogger: ie.NsfLogger,
	}
}

func (s *serviceContext) release() {
	s.NsfLogger.SvcLog.Infoln("Release serviceContext...")

	// Keep persisted services for NSF recovery.
	// dbContext is responsible for closing the database.

	s.NsfLogger.SvcLog.Infoln("serviceContext released")
}

// readProviders loads all providers registered for one service.
// It must be called while the caller holds the appropriate lock.
func (s *serviceContext) readProviders(
	service string,
) ([]serviceProvider, error) {
	exists, err := s.DbIf.Exist(service)
	if err != nil {
		return nil, fmt.Errorf("check service %q: %w", service, err)
	}
	if !exists {
		return nil, nil
	}

	value, err := s.DbIf.Load(service)
	if err != nil {
		return nil, fmt.Errorf("load service %q: %w", service, err)
	}

	var providers []serviceProvider
	if err := json.Unmarshal([]byte(value), &providers); err != nil {
		return nil, fmt.Errorf("decode service %q: %w", service, err)
	}

	return providers, nil
}

func (s *serviceContext) registerServices(
	nfName, sbiAddr string,
	sbiPort int,
	services []string,
) error {
	s.NsfLogger.SvcLog.Tracef("Registering %s services", nfName)

	s.mu.Lock()
	defer s.mu.Unlock()

	updates := make(map[string]string)

	for _, service := range services {
		// Avoid processing duplicate service names.
		if _, ok := updates[service]; ok {
			continue
		}

		providers, err := s.readProviders(service)
		if err != nil {
			return err
		}

		// Prevent duplicate registrations by the same NF.
		registered := false
		for _, provider := range providers {
			if provider.NfName == nfName {
				registered = true
				break
			}
		}

		if !registered {
			providers = append(providers, serviceProvider{
				NfName:     nfName,
				SbiAddress: sbiAddr,
				SbiPort:    sbiPort,
			})
		}

		data, err := json.Marshal(providers)
		if err != nil {
			return fmt.Errorf("encode service %q: %w", service, err)
		}

		updates[service] = string(data)
	}

	if len(updates) == 0 {
		return nil
	}

	if err := s.DbIf.SaveAll(updates); err != nil {
		return fmt.Errorf("save registered services: %w", err)
	}

	return nil
}

func (s *serviceContext) deregisterServices(
	nfName string,
	services []string,
) error {
	s.NsfLogger.SvcLog.Tracef("De-registering %s services", nfName)

	s.mu.Lock()
	defer s.mu.Unlock()

	processed := make(map[string]bool)

	for _, service := range services {
		if processed[service] {
			continue
		}
		processed[service] = true

		providers, err := s.readProviders(service)
		if err != nil {
			return err
		}

		remaining := make([]serviceProvider, 0, len(providers))
		for _, provider := range providers {
			if provider.NfName != nfName {
				remaining = append(remaining, provider)
			}
		}

		if len(remaining) == len(providers) {
			continue
		}

		if len(remaining) == 0 {
			if err := s.DbIf.Remove(service); err != nil {
				return fmt.Errorf("remove service %q: %w", service, err)
			}
			continue
		}

		data, err := json.Marshal(remaining)
		if err != nil {
			return fmt.Errorf("encode service %q: %w", service, err)
		}

		if err := s.DbIf.Save(service, string(data)); err != nil {
			return fmt.Errorf("update service %q: %w", service, err)
		}
	}

	return nil
}

func (s *serviceContext) discoverService(
	service string,
) (string, string, int, error) {
	s.NsfLogger.SvcLog.Tracef("Discovering service %s", service)

	s.mu.RLock()
	defer s.mu.RUnlock()

	providers, err := s.readProviders(service)
	if err != nil {
		return "", "", 0, err
	}

	if len(providers) == 0 {
		return "", "", 0, fmt.Errorf(
			"service %q is not registered", service,
		)
	}

	// Use the first registered provider.
	provider := providers[0]

	return provider.NfName, provider.SbiAddress, provider.SbiPort, nil
}
