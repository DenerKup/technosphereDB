#pragma once

namespace Utils {

template<typename T>
T roundUpDiv(T a, T b)
{
    return (a + b - 1) / b;
}

}
