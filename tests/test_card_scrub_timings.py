import pytest
from personal_data_warehouse import scrub_conversation_cards as cards


def test_phase_records_duration_even_on_failure():
    timings = []
    with pytest.raises(TimeoutError):
        with cards.apply_phase(timings, 'delta_guard'):
            raise TimeoutError()
    assert len(timings) == 1
    assert set(timings[0]) == {'phase', 'seconds'}
    assert timings[0]['phase'] == 'delta_guard'
    assert timings[0]['seconds'] >= 0
