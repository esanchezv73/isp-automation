# 🚀 Containerlab Laboratory: BGP Routing Policies Automation & Telemetry

> A simulated ISP network environment with automated BGP policy failover based on real-time link quality metrics.

---

## 📋 Summary

This laboratory simulates an Internet Service Provider (ISP) network environment with two WAN uplinks (**Provider1** and **Provider2**). It deploys:

- 🔧 An **Automation Framework** for dynamic BGP routing policy updates
- 📊 A **Telemetry Stack** for real-time observability of link quality with upstream providers

✅ **Key Benefit**: The proposed solution enables **automatic BGP routing policy failover** driven by network quality metrics (latency, jitter, packet loss).

---

## 🏗️ Proposed Solution Architecture

### 🤖 Automation Framework

| Component | Role | Key Features |
|-----------|------|-------------|
| **NetBox** | Single Source of Truth (SSoT) | • BGP policy modeling with custom fields<br>• RESTful API for external integrations<br>• Webhooks to trigger automation events<br>🔗 [netboxlabs.com](https://netboxlabs.com/) |
| **GitLab CI/CD** | Configuration Pipeline | • Automated pipeline for BGP policy changes<br>• GitLab Runner as configuration deployment executor<br>🔗 [gitlab.com](https://gitlab.com/) |
| **Nornir** | Automation Orchestrator | • Multi-vendor, multi-platform task orchestration<br>• Integrated with GitLab Runner for secure access to Huawei core routers<br>🔗 [nornir.readthedocs.io](https://nornir.readthedocs.io/en/latest/#) |

---

## 🔄 Architecture Diagram

```mermaid
graph LR
    A[NetBox: SSoT] -->|Webhooks| B(GitLab CI/CD)
    B -->|Trigger Pipeline| C[GitLab Runner]
    C -->|Execute via SSH| D[Nornir]
    D -->|Configure| E[Huawei Core Router]
    F[Telemetry Stack] -->|Metrics| A
    F -->|Alerts| B
