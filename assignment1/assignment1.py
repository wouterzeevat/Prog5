#!/usr/bin/env python3

"""
Composite trapezoidal rule for numerical integration.

Author: Wouter Zeevat
"""

import numpy as np
import argparse

def trapezoid(f, a: float, b: float, *, n: int = 100) -> float:
    """
    Calculate the definite integral of f(x) from a to b using
    the composite trapezoidal rule.

    Args:
        f: Function to integrate.
        a: Lower integration limit.
        b: Upper integration limit.
        n: Number of subdivisions (default: 100).

    Returns:
        Approximation of the definite integral.
    """
    h = (b - a) / n
    integral = (
        (f(a) + f(b))
        + 2 * sum(f(a + i * h) for i in range(1, n))
    ) * h / 2

    return integral


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-a",
        required=True,
        help="Lower bound to trapezoid"
    )

    parser.add_argument(
        "-b",
        required=True,
        help="Lower bound to trapezoid"
    )

    parser.add_argument(
        "-n",
        required=True,
        help="the number of steps to take in your numerical approximation"
    )

    args = parser.parse_args()

    print(trapezoid(np.cos, args.a, args.b, n=args.n))

if __name__ == "__main__":
    main()