# 💳 `Strife: Distributed Payment Gateway System`
*A secure, fault-tolerant payment processing system built with gRPC, implementing enterprise-grade transaction handling*

<div align="center">

**Built with gRPC, Go/Python, and distributed systems principles — featuring secure authentication, idempotent payments, offline transaction queuing, and 2PC distributed transactions.**

---

[![gRPC](https://img.shields.io/badge/gRPC-Protocol-4285f4.svg?style=flat&logo=grpc&logoColor=white)](https://grpc.io)
[![SSL/TLS](https://img.shields.io/badge/SSL%2FTLS-Secure-green.svg?style=flat&logo=lock&logoColor=white)](https://en.wikipedia.org/wiki/Transport_Layer_Security)
[![2PC](https://img.shields.io/badge/2PC-Distributed%20Transactions-blue.svg?style=flat)](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
[![Byzantine Fault Tolerance](https://img.shields.io/badge/BFT-Byzantine%20Fault%20Tolerance-red.svg?style=flat)](https://en.wikipedia.org/wiki/Byzantine_fault)

</div>

---

## 📚 Table of Contents

- [🎯 **Project Overview**](#-project-overview)
- [🏗️ **System Architecture**](#️-system-architecture)
- [⚙️ **Core Components**](#️-core-components)
- [🔐 **Security Features**](#-security-features)
- [🌟 **Advanced Features**](#-advanced-features)
- [🚀 **Quick Start Guide**](#-quick-start-guide)
- [📊 **Load Balancing & Performance**](#-load-balancing--performance)
- [🔧 **Technical Implementation**](#-technical-implementation)

---

## 🎯 Project Overview

**Strife** is a comprehensive **distributed payment gateway system** that mirrors the architecture of modern payment processors like Stripe. Built entirely with gRPC, it implements enterprise-grade features including secure authentication, idempotent payment processing, offline transaction queuing, and distributed consensus mechanisms.

The system demonstrates **critical distributed systems concepts** including fault tolerance, load balancing, secure communication, transaction integrity, and Byzantine fault tolerance — all essential components of modern financial infrastructure.

---

## 🏗️ System Architecture

<div align="center">

### 🔄 **DISTRIBUTED PAYMENT FLOW**

| 👥 **Clients** | 🏦 **Payment Gateway** | 🏛️ **Bank Servers** |
|----------------|------------------------|-------------------|
| Authenticate & authorize | Route transactions securely | Process bank operations |
| Queue offline payments | Load balance requests | Handle account validation |
| Handle network failures | Implement 2PC coordination | Maintain transaction logs |

</div>

### 🌐 **Multi-Tier Architecture**
- **Client Layer**: Secure authentication, offline payment queuing, retry mechanisms
- **Gateway Layer**: Load balancing, transaction coordination, security enforcement
- **Bank Layer**: Distributed banking services with fault tolerance
- **Security Layer**: SSL/TLS encryption, certificate-based authentication

---

## ⚙️ Core Components

### 🏦 **Payment Gateway**
The central orchestrator that manages all payment transactions, implements load balancing strategies, and coordinates distributed transactions across multiple bank servers.

### 🏛️ **Bank Servers** 
Independent banking services that handle account operations, transaction processing, and maintain user account data with robust fault tolerance.

### 👥 **Client Applications**
Intelligent payment clients featuring secure authentication, offline transaction queuing, and automatic retry mechanisms for network failures.

### ⚖️ **Load Balancer**
Advanced load balancing system supporting multiple strategies (Pick First, Round Robin, Least Load) with dynamic server discovery and health monitoring.

---

## 🔐 Security Features

### 🔒 **SSL/TLS Mutual Authentication**
- **Certificate-based security** with custom CA and client certificates
- **Encrypted communication** for all client-gateway and gateway-bank interactions
- **Identity verification** ensuring only authorized clients can access the system

### 🛡️ **Authorization & Access Control**
- **Role-based permissions** implemented through gRPC interceptors
- **Resource-level authorization** restricting users to their own account data
- **Transaction limits** based on available account balance

### 📝 **Comprehensive Logging**
- **Request/Response logging** for all gRPC interactions
- **Transaction audit trails** with detailed metadata
- **Error tracking** and debugging information
- **Performance monitoring** and system health metrics

---

## 🌟 Advanced Features

### 🔄 **Idempotent Payment Processing**
- **Duplicate transaction prevention** using sophisticated deduplication algorithms
- **Scalable idempotency keys** avoiding simple timestamp-based solutions
- **Network failure resilience** ensuring exactly-once payment semantics
- **Retry safety** guaranteeing consistent transaction outcomes

### 📱 **Offline Payment Queuing**
- **Client-side transaction queue** for handling network outages
- **Automatic retry mechanisms** with exponential backoff
- **Connection recovery** with seamless payment resumption
- **Persistent queue storage** maintaining payment integrity

### 🤝 **Two-Phase Commit (2PC) Transactions**
- **Distributed transaction coordination** across multiple bank servers
- **Atomic transaction processing** ensuring all-or-nothing semantics
- **Configurable timeout handling** with graceful abort mechanisms
- **Participant failure recovery** maintaining system consistency

### 🛡️ **Byzantine Fault Tolerance (Bonus)**
- **Consensus algorithm implementation** handling malicious or faulty nodes
- **Multi-round verification** ensuring agreement among honest participants
- **Fault tolerance** supporting up to ⌊(n-1)/3⌋ faulty nodes
- **Distributed decision making** with majority voting mechanisms

---

## 🚀 Quick Start Guide

### 📋 **Prerequisites**
- Go 1.19+ or Python 3.8+ installed
- OpenSSL for certificate generation
- Multiple terminal windows for distributed components

### 🛠️ **Step-by-Step Setup**

#### **1. Generate Protocol Buffer Code**
```bash
# For Go
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/*.proto

# For Python
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/*.proto
```

#### **2. Generate SSL/TLS Certificates**
```bash
# Generate CA certificate
openssl req -x509 -newkey rsa:4096 -keyout ca-key.pem -out ca-cert.pem -days 365 -nodes

# Generate server certificate
openssl req -newkey rsa:4096 -keyout server-key.pem -out server-req.pem -nodes
openssl x509 -req -in server-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -days 365

# Generate client certificate
openssl req -newkey rsa:4096 -keyout client-key.pem -out client-req.pem -nodes
openssl x509 -req -in client-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem -days 365
```

#### **3. Setup Load Balancer**
```bash
# Terminal 1 - Start Load Balancer
go run loadbalancer/main.go --policy=round_robin

# Or test different policies
go run loadbalancer/main.go --policy=least_load
go run loadbalancer/main.go --policy=pick_first
```

#### **4. Deploy Bank Servers**
Start multiple bank servers in separate terminals:
```bash
# Terminal 2 - Bank Server 1
go run bank/server.go --bank-name=BankOfAmerica --port=8081

# Terminal 3 - Bank Server 2
go run bank/server.go --bank-name=ChaseBank --port=8082

# Terminal 4 - Bank Server 3
go run bank/server.go --bank-name=WellsFargo --port=8083
```

#### **5. Launch Payment Gateway**
```bash
# Terminal 5 - Payment Gateway
go run gateway/main.go --port=8080 --ssl-cert=server-cert.pem --ssl-key=server-key.pem
```

#### **6. Start Client Applications**
```bash
# Terminal 6 - Client 1
go run client/main.go --username=john_doe --gateway-addr=localhost:8080

# Terminal 7 - Client 2 (optional)
go run client/main.go --username=jane_smith --gateway-addr=localhost:8080
```

---

## 📊 Load Balancing & Performance

### ⚖️ **Load Balancing Strategies**

<div align="center">

| 🎯 **Strategy** | 📋 **Implementation** | 🚀 **Use Case** |
|----------------|----------------------|-----------------|
| **Pick First** | Selects first available server | Simple failover scenarios |
| **Round Robin** | Cyclical distribution | Even load distribution |
| **Least Load** | Routes to least busy server | CPU-intensive operations |

</div>

### 📈 **Performance Testing**
- **Multi-client simulation** with 100+ concurrent clients
- **Multi-server deployment** with 10-15 bank servers
- **Throughput measurement** and response time analysis
- **Load distribution visualization** with comprehensive graphs

### 🔍 **Scaling Capabilities**
- **Horizontal scaling** with dynamic server addition
- **Automatic failover** and health monitoring
- **Resource utilization** tracking and optimization
- **Bottleneck identification** and performance tuning

---

## 🔧 Technical Implementation

### 📡 **gRPC Services**
```protobuf
// Payment Gateway Service
service PaymentGateway {
    rpc ProcessPayment(PaymentRequest) returns (PaymentResponse);
    rpc GetBalance(BalanceRequest) returns (BalanceResponse);
    rpc GetTransactionHistory(HistoryRequest) returns (HistoryResponse);
}

// Bank Service
service BankService {
    rpc ValidateAccount(AccountRequest) returns (AccountResponse);
    rpc ProcessTransaction(TransactionRequest) returns (TransactionResponse);
    rpc GetAccountBalance(BalanceRequest) returns (BalanceResponse);
}

// Load Balancer Service
service LoadBalancer {
    rpc GetBestServer(ServerRequest) returns (ServerResponse);
    rpc ReportServerLoad(LoadReport) returns (LoadAck);
}
```

### 🏗️ **Architecture Patterns**
- **Microservices architecture** with clear service boundaries
- **Event-driven design** for asynchronous processing
- **Circuit breaker pattern** for fault tolerance
- **Retry mechanisms** with exponential backoff

### 🛠️ **Technology Stack**
- **`gRPC`**: High-performance RPC framework
- **`Protocol Buffers`**: Efficient data serialization
- **`SSL/TLS`**: Secure communication layer
- **`Go/Python`**: Concurrent programming languages
- **`2PC Protocol`**: Distributed transaction coordination

---

<div align="center">


**Key Features:**
- 🔐 **SSL/TLS Security** | 🔄 **Idempotent Payments** | 📱 **Offline Queuing** | 🤝 **2PC Transactions** | 🛡️ **Multi-Level Authentication**

---

### 🚀 **This was an attempt to Learn and Implement Distributed Payment Gateway. This was part of course CS3.401 : Distributed Systems**

---

*Engineered for high-performance distributed payment processing*

</div>