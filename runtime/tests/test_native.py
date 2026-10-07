from kabudachi import _native


def test_native_version_matches_python_version():
    import kabudachi

    assert _native.version() == kabudachi.__version__


def test_a_result_digest_is_blake3():
    # BLAKE3 of the empty input, from the BLAKE3 reference test vectors.
    assert _native.result_digest(b"") == bytes.fromhex(
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    )
