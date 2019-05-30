cmake_minimum_required(VERSION 3.12)
project(harp_thirdparty)

add_library(harp_thirdparty STATIC
        SpBLAS/nist_spblas.cpp
        SpDM3/src/mmio.cpp
        radix/copy_play.cpp
        radix/pr.cpp
        radix/commons/benchmark.cpp
        radix/commons/util.cpp
        SpMP/COO.cpp
        SpMP/CSR.cpp
        SpMP/gs_spmv_fused.cpp
        SpMP/Laplacian.cpp
        SpMP/LevelSchedule.cpp
        SpMP/MemoryPool.cpp
        SpMP/mm_io.cpp
        SpMP/Permute.cpp
        SpMP/PrefixSum.cpp
        SpMP/SpMV.cpp
        SpMP/SymGS.cpp
        SpMP/Utils.cpp
        SpMP/Vector.cpp
        SpMP/reordering/BFSBipartite.cpp
        SpMP/reordering/ConnectedComponents.cpp
        SpMP/reordering/RCM.cpp
        SpMP/synk/barrier.cpp
        SpMP/synk/loadimba.cpp
        SpMP/synk/synk.cpp
#        graph500-1.2/generator/btrd_binomial_distribution.c
#        graph500-1.2/generator/splittable_mrg.c
#        graph500-1.2/generator/mrg_transitions.c
#        graph500-1.2/generator/graph_generator.c
#        graph500-1.2/generator/permutation_gen.c
#        graph500-1.2/generator/make_graph.c
#        graph500-1.2/generator/utils.c
#        graph500-1.2/generator/scramble_edges.c
        )

target_include_directories(harp_thirdparty PUBLIC
        SpBLAS
        SpDM3/include
        SpMP
        SpMP/reordering
        SpMP/synk
        radix
        radix/commons
        )

