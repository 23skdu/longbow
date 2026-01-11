package store

// EnableRepairAgent starts the connectivity repair agent with the given configuration.
// This is optional and disabled by default. Call this after index initialization if you want
// automatic detection and repair of disconnected sub-graphs.
func (h *ArrowHNSW) EnableRepairAgent(config RepairAgentConfig) {
	if h.repairAgent != nil {
		h.repairAgent.Stop() // Stop existing agent if any
	}

	config.Enabled = true // Force enabled
	h.repairAgent = NewRepairAgent(h, config)
	h.repairAgent.Start()
}

// DisableRepairAgent stops the connectivity repair agent
func (h *ArrowHNSW) DisableRepairAgent() {
	if h.repairAgent != nil {
		h.repairAgent.Stop()
	}
}
