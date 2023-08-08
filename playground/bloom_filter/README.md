The size of a Bloom filter m, for n items and a desired false positive probability p, can be calculated using the formula:

m = -((n ln p) / (ln 2)^2)

where ln is the natural logarithm.

We want to store about a billion (n = 10^9) Bitcoin transaction IDs
If we can tolerate a false positive probability of 1% (p = 0.01):

m = -((10^9 * ln(0.01)) / (ln(2)^2))

In bits, this comes out to approximately 96 billion bits or about 12 GB.

This calculation assumes that we are using an optimal number of hash functions, which is approximately (m/n) ln 2. In this case, that would be about 10 hash functions.

Implementing 10 different hash functions would be quite involved. One typical approach is to use two independent hash functions to generate the required number of hashes. This technique is described in Kirsch and Mitzenmacher (2008) and it suggests that the ith hash function can be computed as h(i) = h1(x) + i*h2(x).
