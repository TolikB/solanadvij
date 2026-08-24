from __future__ import annotations

import random


class DeterministicIdFactory:
    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    def __call__(self) -> str:
        return f"id-{self._rng.getrandbits(64):016x}"
