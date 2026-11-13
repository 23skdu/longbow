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
from .range import RangeScenarioMixin
from .federated import FederatedScenarioMixin
from .text import TextScenarioMixin
from .multimodal import MultiModalScenarioMixin

class ScenarioMixin(
    RecommendScenarioMixin,
    DeletionScenarioMixin,
    GraphragScenarioMixin,
    ExchangeScenarioMixin,
    OnnxScenarioMixin,
    TemporalScenarioMixin,
    GeoScenarioMixin,
    ChurnScenarioMixin,
    ClusterScenarioMixin,
    LearnedIndexScenarioMixin,
    RangeScenarioMixin,
    FederatedScenarioMixin,
    TextScenarioMixin,
    MultiModalScenarioMixin,
):
    pass

__all__ = [
    "ScenarioMixin",
    "RecommendScenarioMixin",
    "DeletionScenarioMixin",
    "GraphragScenarioMixin",
    "ExchangeScenarioMixin",
    "OnnxScenarioMixin",
    "TemporalScenarioMixin",
    "GeoScenarioMixin",
    "ChurnScenarioMixin",
    "ClusterScenarioMixin",
    "LearnedIndexScenarioMixin",
    "RangeScenarioMixin",
    "FederatedScenarioMixin",
    "TextScenarioMixin",
    "MultiModalScenarioMixin",
]
