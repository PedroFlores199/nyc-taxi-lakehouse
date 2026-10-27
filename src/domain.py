import re
from dataclasses import dataclass
from datetime import date

TIME_SLOTS: tuple[tuple[str, int, int], ...] = (
    ("madrugada", 0, 5),
    ("punta_manana", 6, 9),
    ("mediodia", 10, 15),
    ("punta_tarde", 16, 19),
    ("noche", 20, 23),
)

DAY_NAMES: tuple[str, ...] = (
    "lunes",
    "martes",
    "miércoles",
    "jueves",
    "viernes",
    "sábado",
    "domingo",
)

_YEAR_MONTH_PATTERN = re.compile(r"^(\d{4})-(\d{2})$")


def time_slot_for_hour(hour: int) -> str:
    for name, start, end in TIME_SLOTS:
        if start <= hour <= end:
            return name
    raise ValueError(f"Hora fuera de rango (0-23): {hour}")


@dataclass(frozen=True, order=True)
class YearMonth:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"Mes fuera de rango (1-12): {self.month}")
        if self.year < 2009:
            raise ValueError(f"No hay datos de TLC anteriores a 2009: {self.year}")

    @classmethod
    def parse(cls, value: str) -> "YearMonth":
        match = _YEAR_MONTH_PATTERN.match(value.strip())
        if match is None:
            raise ValueError(f"Formato de mes no válido (se espera AAAA-MM): '{value}'")
        return cls(int(match.group(1)), int(match.group(2)))

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def first_day(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def next(self) -> "YearMonth":
        if self.month == 12:
            return YearMonth(self.year + 1, 1)
        return YearMonth(self.year, self.month + 1)

    @property
    def previous(self) -> "YearMonth":
        if self.month == 1:
            return YearMonth(self.year - 1, 12)
        return YearMonth(self.year, self.month - 1)


def month_range(start: YearMonth, end: YearMonth) -> list[YearMonth]:
    if end < start:
        raise ValueError(f"El mes final {end} es anterior al inicial {start}")
    months = [start]
    while months[-1] < end:
        months.append(months[-1].next)
    return months
