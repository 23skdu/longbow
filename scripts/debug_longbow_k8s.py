#!/usr/bin/env python3
import sys
import argparse
import os
try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException
except ImportError:
    print("Error: The 'kubernetes' Python module is not installed.")
    print("Please install it with: pip install kubernetes")
    sys.exit(1)

def get_longbow_resources(namespace, label_selector):
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    
    print(f"\n" + "="*60)
    print(f"DEBUGGING LONGBOW IN NAMESPACE: {namespace}")
    print(f"SELECTOR: {label_selector}")
    print("="*60 + "\n")
    
    # 1. Check Deployments
    print("--- 1. Deployments ---")
    try:
        deployments = apps_v1.list_namespaced_deployment(namespace, label_selector=label_selector)
        if not deployments.items:
            print(f"  No Longbow deployments found with selector: {label_selector}")
        for dep in deployments.items:
            print(f"  Deployment: {dep.metadata.name}")
            print(f"    Replicas: {dep.status.replicas or 0} total, {dep.status.available_replicas or 0} available, {dep.status.unavailable_replicas or 0} unavailable")
            if dep.status.conditions:
                for cond in dep.status.conditions:
                    if cond.status != "True" and cond.type != "Progressing":
                        print(f"    [WARNING] Condition {cond.type} is {cond.status}")
                        print(f"              Reason: {cond.reason}")
                        print(f"              Message: {cond.message}")
    except ApiException as e:
        print(f"  Error listing deployments: {e}")

    # 2. Check Pods
    print("\n--- 2. Pods ---")
    try:
        pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)
        if not pods.items:
            print(f"  No Longbow pods found with selector: {label_selector}")
        for pod in pods.items:
            print(f"\n  Pod: {pod.metadata.name}")
            print(f"    Phase: {pod.status.phase}")
            print(f"    IP: {pod.status.pod_ip}")
            
            # Check Container Statuses
            if pod.status.container_statuses:
                for cs in pod.status.container_statuses:
                    print(f"    Container: {cs.name}")
                    print(f"      Ready: {cs.ready}")
                    print(f"      Restart Count: {cs.restart_count}")
                    if cs.state.waiting:
                        print(f"      [WAITING] Reason: {cs.state.waiting.reason}")
                        print(f"                Message: {cs.state.waiting.message}")
                    if cs.state.terminated:
                        print(f"      [TERMINATED] Reason: {cs.state.terminated.reason}")
                        print(f"                   Exit Code: {cs.state.terminated.exit_code}")
                        print(f"                   Message: {cs.state.terminated.message}")
            
            # Check Events if pod is not running or has restarts
            needs_events = pod.status.phase != "Running"
            if pod.status.container_statuses:
                if any(cs.restart_count > 0 or not cs.ready for cs in pod.status.container_statuses):
                    needs_events = True
            
            if needs_events:
                print("    --- Recent Events ---")
                try:
                    events = v1.list_namespaced_event(namespace, field_selector=f"involvedObject.name={pod.metadata.name}")
                    if not events.items:
                        print("      No events found.")
                    for event in events.items:
                        print(f"      {event.type} {event.reason}: {event.message}")
                except ApiException as e:
                    print(f"      Error fetching events: {e}")
                
                # Fetch Logs if it crashed or is not ready
                print("    --- Logs (Last 50 lines) ---")
                for cs in pod.status.container_statuses or []:
                    print(f"      [Container: {cs.name}]")
                    # Try previous logs first if it restarted
                    if cs.restart_count > 0:
                        try:
                            logs = v1.read_namespaced_pod_log(pod.metadata.name, namespace, container=cs.name, previous=True, tail_lines=50)
                            print("      --- Previous Logs ---")
                            for line in logs.splitlines():
                                print(f"        {line}")
                        except ApiException:
                            pass
                    
                    # Current logs
                    try:
                        logs = v1.read_namespaced_pod_log(pod.metadata.name, namespace, container=cs.name, tail_lines=50)
                        print("      --- Current Logs ---")
                        for line in logs.splitlines():
                            print(f"        {line}")
                    except ApiException as e:
                        print(f"        Error fetching logs: {e.reason}")

    except ApiException as e:
        print(f"  Error listing pods: {e}")

    # 3. Check Services
    print("\n--- 3. Services ---")
    try:
        services = v1.list_namespaced_service(namespace, label_selector=label_selector)
        if not services.items:
            print("  No Longbow services found.")
        for svc in services.items:
            print(f"  Service: {svc.metadata.name}")
            print(f"    Type: {svc.spec.type}")
            print(f"    ClusterIP: {svc.spec.cluster_ip}")
            ports = [f"{p.port}:{p.target_port}/{p.protocol}" for p in svc.spec.ports]
            print(f"    Ports: {', '.join(ports)}")
            if svc.status.load_balancer and svc.status.load_balancer.ingress:
                for ingress in svc.status.load_balancer.ingress:
                    print(f"    LoadBalancer Ingress: {ingress.ip or ingress.hostname}")
    except ApiException as e:
        print(f"  Error listing services: {e}")

def main():
    parser = argparse.ArgumentParser(description="Debug Longbow Kubernetes Deployments")
    parser.add_argument("-n", "--namespace", default=os.environ.get("KUBECONFIG_NAMESPACE", "default"), help="Kubernetes namespace (default: default)")
    parser.add_argument("-l", "--label", default="app.kubernetes.io/name=longbow", help="Label selector (default: app.kubernetes.io/name=longbow)")
    
    args = parser.parse_args()
    
    try:
        # Try local kubeconfig
        config.load_kube_config()
    except Exception:
        try:
            # Try in-cluster config
            config.load_incluster_config()
        except Exception as e:
            print(f"Error: Could not load kubernetes configuration.")
            print(f"Reason: {e}")
            sys.exit(1)
            
    get_longbow_resources(args.namespace, args.label)

if __name__ == "__main__":
    main()
