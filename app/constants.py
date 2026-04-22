"""
Application constants: chart palette and optional dependency flags.
"""
from importlib.util import find_spec

HAS_PARAMIKO = find_spec("paramiko") is not None
HAS_PSUTIL = find_spec("psutil") is not None
HAS_SKLEARN = find_spec("sklearn.ensemble") is not None

# Station color palette used in Plotly charts.
STATION_COLORS = [
    "#3B82F6",  # Blue
    "#EF4444",  # Red
    "#10B981",  # Green
    "#F59E0B",  # Amber
    "#8B5CF6",  # Purple
    "#EC4899",  # Pink
    "#06B6D4",  # Cyan
    "#F97316",  # Orange
    "#14B8A6",  # Teal
    "#A855F7",  # Violet
]
