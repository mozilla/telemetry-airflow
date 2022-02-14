"""
Module with Airflow tag definitions
"""

from enum import Enum


class InvalidTagError(AttributeError):
    pass

class Tag(Enum):
    """
    Enum containing available Airflow tags
    """

    def __getattr__(self, item: str) -> str:
        """
        This is to simplify accessing enum values.
        Instead of Tag.ImpactTier.value.tier_1.value we can just use
        Tag.ImpactTier.tier_1

        # source: https://newbedev.com/enum-of-enums-in-python
        """

        if item == '_value_':
            raise InvalidTagError

        try:
            ret_val = getattr(self.value, item).value
        except AttributeError as _err:
            raise InvalidTagError(_err)

        return ret_val


    class ImpactTier(Enum):
        """
        Valid options for Impact tier tag
        """

        tier_1 = "impact/tier_1"
        tier_2 = "impact/tier_2"
        tier_3 = "impact/tier_3"
