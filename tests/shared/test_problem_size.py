"""The one size limit every entry point applies: participants x timesteps x iterations."""

from shared.problem_size import MAX_SIMULATION_CELLS, too_large


def test_run_at_the_limit_is_accepted():
    assert too_large(1, MAX_SIMULATION_CELLS, 1) is None
    assert too_large(2, MAX_SIMULATION_CELLS // 2, 1) is None


def test_one_value_over_the_limit_is_refused():
    assert too_large(1, MAX_SIMULATION_CELLS + 1, 1) is not None


def test_every_factor_counts():
    # A third of the limit per factor combination that multiplies past it.
    third = MAX_SIMULATION_CELLS // 3 + 1
    assert too_large(third, 1, 3) is not None
    assert too_large(1, third, 3) is not None
    assert too_large(3, third, 1) is not None
    assert too_large(third, 1, 2) is None


def test_detail_names_the_factors_and_the_limit():
    detail = too_large(142, 35_040, 3)
    assert detail is not None
    assert "142 participants x 35040 timesteps x 3 iterations" in detail
    assert f"= {142 * 35_040 * 3} cells" in detail
    assert str(MAX_SIMULATION_CELLS) in detail


def test_the_limit_matches_the_crm_reading_limit():
    # One iteration may use every reading the CRM read allows (shared/problem_size.py).
    from shared.crm_preflight import MAX_READING_ROWS

    assert MAX_SIMULATION_CELLS == MAX_READING_ROWS
