from handlers import _extract_large_file_source


def test_extract_large_file_source_uses_chat_and_msg_ids():
    current_file = {"chat_id": 12345, "msg_id": 67890}
    assert _extract_large_file_source(current_file) == (12345, 67890)


def test_extract_large_file_source_uses_forward_metadata_when_present():
    current_file = {
        "forward": {"chat_id": -100111, "message_id": 22},
    }
    assert _extract_large_file_source(current_file) == (-100111, 22)
