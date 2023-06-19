# Importing Modules
from txtai.pipeline import HFOnnx

# Initializing HFOnnx
onnx = HFOnnx()

# Converting Model
onnx_model = onnx(
    "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
    "pooling",
    "embeddings.onnx",
    quantize=True)
