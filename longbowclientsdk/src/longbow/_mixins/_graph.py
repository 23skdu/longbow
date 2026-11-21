"""Graph edges, traversal, centrality, communities, GraphRAG, and recommendations."""

import pyarrow.flight as flight
import json
from typing import List, Dict, Any
from ..exceptions import (
    LongbowQueryError,
)


class GraphMixin:
    def recommend(
        self,
        dataset: str,
        seed_ids: List[str],
        k: int = 10,
        alpha: float = 0.5,
        max_hops: int = 2,
        decay: float = 0.5,
    ) -> "pd.DataFrame":
        """
        Produce a list of recommendations based on seed IDs (hybrid vector-graph closeness).

        Args:
            dataset: The dataset name.
            seed_ids: List of source IDs to use as seeds.
            k: Number of recommendations to return.
            alpha: Hybrid blend (1.0 = pure vector similarity, 0.0 = pure graph connectivity).
            max_hops: BFS depth for graph connectivity.
            decay: Multi-hop connectivity decay factor.

        Returns:
            Pandas DataFrame with 'id' and 'score'.
        """
        if self._data_client is None:
            self.connect()

        req = {
            "dataset": dataset,
            "seed_ids": seed_ids,
            "k": k,
            "alpha": float(alpha),
            "max_hops": int(max_hops),
            "decay": float(decay),
        }

        ticket_json = {"recommend": req}
        ticket = flight.Ticket(json.dumps(ticket_json).encode("utf-8"))

        try:
            reader = self._require_data_client().do_get(
                ticket, options=self._get_call_options()
            )
            table = reader.read_all()
            return table.to_pandas()
        except Exception as e:
            raise LongbowQueryError(f"Recommendation failed: {e}")
    def add_edge(
        self,
        dataset: str,
        subject: int,
        predicate: str,
        object: int,
        weight: float = 1.0,
    ) -> None:
        """Add a directed edge to the graph."""
        req: Dict[str, Any] = {
            "dataset": dataset,
            "subject": subject,
            "predicate": predicate,
            "object": object,
            "weight": weight,
        }
        action = flight.Action("add-edge", json.dumps(req).encode("utf-8"))
        try:
            list(
                self._require_meta_client().do_action(
                    action, options=self._get_call_options()
                )
            )
        except Exception as e:
            raise LongbowQueryError(f"Add edge failed: {e}")
    def traverse(
        self,
        dataset: str,
        start: int,
        max_hops: int = 2,
        incoming: bool = False,
        decay: float = 0.0,
        weighted: bool = True,
    ) -> List[Dict]:
        """Traverse the graph from a start node."""
        req: Dict[str, Any] = {
            "dataset": dataset,
            "start": start,
            "max_hops": max_hops,
            "incoming": incoming,
            "weighted": weighted,
            "decay": decay,
        }
        action = flight.Action("traverse-graph", json.dumps(req).encode("utf-8"))
        try:
            results = []
            for res in self._require_meta_client().do_action(
                action, options=self._get_call_options()
            ):
                results.append(json.loads(res.body.to_pybytes()))
            return results
        except Exception as e:
            raise LongbowQueryError(f"Traversal failed: {e}")
    def get_graph_stats(self, dataset: str) -> Dict[str, Any]:
        """Get graph statistics."""
        return self._do_action_json("GetGraphStats", {"dataset": dataset}, default={})
    def calculate_pagerank(
        self,
        dataset: str,
        damping_factor: float = 0.85,
        max_iterations: int = 20,
        tolerance: float = 1e-4,
    ) -> Dict[str, float]:
        """
        Calculate PageRank centrality for all nodes in the dataset's HNSW graph.

        Args:
            dataset: Name of the dataset.
            damping_factor: Damping coefficient (default 0.85).
            max_iterations: Maximum iterations (default 20).
            tolerance: Convergence tolerance (default 1e-4).

        Returns:
            Dict[str, float]: Mapping of node IDs to PageRank scores.
        """
        raw_res = self._do_action_json(
            "calculate-pagerank",
            {
                "dataset": dataset,
                "damping_factor": damping_factor,
                "max_iterations": max_iterations,
                "tolerance": tolerance,
            },
            default={},
        )
        return raw_res.get("Scores", {})
    def detect_communities(self, dataset: str, max_iterations: int = 10) -> Dict[str, Any]:
        """
        Run community detection (LPA) on the dataset's HNSW graph.

        Args:
            dataset: Name of the dataset.
            max_iterations: Maximum iterations for propagation (default 10).

        Returns:
            Dict[str, Any]: Result containing 'CommunityCount' and 'Labels' mapping.
        """
        return self._do_action_json(
            "detect-communities",
            {"dataset": dataset, "max_iterations": max_iterations},
            default={},
        )
    def graph_rag_expand(self, dataset: str, node_ids: List[int]) -> Dict[int, List[int]]:
        """
        Perform a distributed GraphRAG expansion (get neighbors for multiple nodes).
        
        Args:
            dataset: Name of the dataset.
            node_ids: List of node IDs to expand.
            
        Returns:
            Dict[int, List[int]]: Mapping of node IDs to their neighbor lists.
        """
        return self._do_action_json(
            "GraphRAGExpand", {"dataset": dataset, "node_ids": node_ids}, default={}
        )
