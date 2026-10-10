from .recommend import RecommendScenarioMixin
from .deletion import DeletionScenarioMixin
from .graphrag import GraphragScenarioMixin
from .exchange import ExchangeScenarioMixin
from .onnx import OnnxScenarioMixin
from .temporal import TemporalScenarioMixin
from .geo import GeoScenarioMixin
from .churn import ChurnScenarioMixin
from .cluster import ClusterScenarioMixin
from .learned_index import LearnedIndexScenarioMixin

class ScenarioMixin(RecommendScenarioMixin, DeletionScenarioMixin, GraphragScenarioMixin, ExchangeScenarioMixin, OnnxScenarioMixin, TemporalScenarioMixin, GeoScenarioMixin, ChurnScenarioMixin, ClusterScenarioMixin, LearnedIndexScenarioMixin):
    pass

__all__ = ["ScenarioMixin"] + ["RecommendScenarioMixin", "DeletionScenarioMixin", "GraphragScenarioMixin", "ExchangeScenarioMixin", "OnnxScenarioMixin", "TemporalScenarioMixin", "GeoScenarioMixin", "ChurnScenarioMixin", "ClusterScenarioMixin", "LearnedIndexScenarioMixin"]
