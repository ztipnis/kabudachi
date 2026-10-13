"""Build settings shared by the Python extensions in this workspace."""

# A macOS extension leaves the Python symbols to the interpreter that
# loads it, and needs the pre-chained-fixups Mach-O format
# (https://pyo3.rs/main/building-and-distribution#macos,
# https://github.com/PyO3/pyo3/issues/5035). The C toolchain links
# for the macOS of the building machine; the wheel's macOS tag
# (//runtime:kabudachi_wheel) promises 11.0, so say so.
PYTHON_EXTENSION_RUSTC_FLAGS = select({
    "@platforms//os:macos": [
        "-C",
        "link-arg=-undefined",
        "-C",
        "link-arg=dynamic_lookup",
        "-C",
        "link-arg=-Wl,-no_fixup_chains",
        "-C",
        "link-arg=-mmacosx-version-min=11.0",
    ],
    "//conditions:default": [],
})
