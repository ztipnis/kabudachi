from kabudachi import _native


def test_native_version_matches_python_version():
    import kabudachi

    assert _native.version() == kabudachi.__version__
