import pytest
from dags.utils.tags import Tag, InvalidTagError


@pytest.mark.parametrize(
    "actual,expected",
    [
        (Tag.ImpactTier.tier_1, "impact/tier_1"),
        (Tag.ImpactTier.tier_2, "impact/tier_2"),
        (Tag.ImpactTier.tier_3, "impact/tier_3"),
    ]
)
def test_valid_impact_tag(actual, expected):
    assert actual == expected


@pytest.mark.parametrize(
    "obj,attr,expected",
    [
        (Tag.ImpactTier, "tier_1", "impact/tier_1"),
        (Tag.ImpactTier, "tier_2", "impact/tier_2"),
        (Tag.ImpactTier, "tier_3", "impact/tier_3"),
    ]
)
def test_valid_impact_tag(obj, attr, expected):
    assert getattr(obj, attr) == expected


@pytest.mark.parametrize(
    "invalid_input",
    [
        "tier_4",
        "",
        "bq-etl",
    ]
)
def test_valid_impact_tag(invalid_input):
    with pytest.raises(InvalidTagError):
        getattr(Tag.ImpactTier, invalid_input)
