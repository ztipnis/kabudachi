# Merges the compiled //bindings:_native extension (a separate Bazel
# package/sys.path root) into this package as `kabudachi._native`.
# Bazel dev/test-layout glue only — see README §23.15 ("Packaging:
# maturin") for the real packaging story and why this may not be needed.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

__version__ = "0.1.0"
