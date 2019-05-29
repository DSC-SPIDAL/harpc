#ifndef PARTITIONER_H_
#define PARTITIONER_H_

#include <algorithm>
#include <cinttypes>
#include <cstdlib>
#include <iostream>
#include <utility>

#include "pvector.h"
#include "commons/benchmark.h"
#include <immintrin.h>

/*
Todo:
 - auto determination of bin size and shamts
 - parallelize
*/

template <typename NodeID_, typename Payload_>
class Partitioner {
public:
  typedef std::pair<NodeID_, Payload_> NodePayloadPair;

  Partitioner(int log_width_bytes, const RGraph &g) :
      num_bins_(CalcNumBins(log_width_bytes, g)), bins_(num_bins_),
      buffer_tails_(num_bins_) {
    shamt_ = 0;
    size_t bin_width = (1l << log_width_bytes) / sizeof(Payload_);
    while ((1l<<shamt_) < bin_width)
      shamt_++;
    std::cout << "bins:  " << num_bins_ << std::endl;
    std::cout << "shamt: " << shamt_ << std::endl;
    std::cout << (1l<<shamt_) * num_bins_ << " " << g.num_nodes() << std::endl;
    pvector<size_t> bin_sizes = BinSizeHistogram(g);
    size_t buffer_size_bytes = num_bins_ * kBufferSize_*sizeof(NodePayloadPair);
    buffer_ = (NodePayloadPair*) aligned_alloc(64, buffer_size_bytes);
    for (int b=0; b < num_bins_; b++) {
      bins_[b].resize(bin_sizes[b]);
      TouchMem(bins_[b]);
      bins_[b].clear();
      buffer_tails_[b] = b * kBufferSize_;
    }
  }

  ~Partitioner() {
    delete[] buffer_;
  }

  void SendMsg(const NodeID_ dest, const Payload_ data) {
    const int b = CalcBin(dest);
    buffer_[buffer_tails_[b]++] = std::make_pair(dest, data);
    if (buffer_tails_[b] == (b+1)*kBufferSize_) {
      const size_t copy_size_bytes = kBufferSize_ * sizeof(NodePayloadPair);
      AVXmemcpy(bins_[b].end(), buffer_ + b*kBufferSize_, copy_size_bytes);
      buffer_tails_[b] = b*kBufferSize_;
      bins_[b].resize(bins_[b].size() + kBufferSize_);
    }
  }

  void Flush() {
    for (int b=0; b < num_bins_; b++) {
      const size_t elements_left = buffer_tails_[b] - b*kBufferSize_;
      memcpy(bins_[b].end(), buffer_ + b*kBufferSize_,
             elements_left * sizeof(NodePayloadPair));
      bins_[b].resize(bins_[b].size() + elements_left);
      buffer_tails_[b] = b * kBufferSize_;
    }
  }

  int get_num_bins() const {
    return num_bins_;
  }

  pvector<NodePayloadPair>& GetBin(int b) {
    return bins_[b];
  }


private:
  int num_bins_;
  int shamt_;
  const static int kBufferSize_ = 32;
  pvector<pvector<NodePayloadPair>> bins_;
  NodePayloadPair* buffer_;
  pvector<size_t> buffer_tails_;

  static
  int64_t CalcNumBins(int log_width_bytes, const RGraph &g) {
    int64_t num_elements = (1l << log_width_bytes) / sizeof(Payload_);
    return (g.num_nodes() + num_elements - 1) / num_elements;
  }

  int CalcBin(NodeID_ dest) {
    return dest >> shamt_;
  }

  pvector<size_t> BinSizeHistogram(const RGraph &g) {
    pvector<size_t> totals(num_bins_, 0);
    for (NodeID_ n=0; n<g.num_nodes(); n++) {
      totals[CalcBin(n)] += g.in_degree(n);
    }
    return totals;
  }

  void TouchMem(pvector<NodePayloadPair> &bin) {
    const size_t stride = 4096 / sizeof(NodePayloadPair);
    for (size_t i=0; i<bin.size(); i+=stride) {
      bin[i] = std::make_pair(-1,0);
    }
  }

  void AVXmemcpy(void* dst, void* src, const uint64_t size) const {
    float* f_dst = (float*) dst;
    float* f_src = (float*) src;
    for (uint64_t i=0; i<size; i+=sizeof(__m256)) {
      _mm256_stream_ps(f_dst, _mm256_load_ps(f_src));
      // _mm256_store_ps(f_dst, _mm256_load_ps(f_src));
      f_dst += sizeof(__m256) / sizeof(float);
      f_src += sizeof(__m256) / sizeof(float);
    }
  }

  void SSEmemcpy(void* dst, void* src, uint64_t size) {
    __m128i* f_dst = (__m128i*) dst;
    __m128i* f_src = (__m128i*) src;
    for (uint64_t i=0; i<size; i+=sizeof(__m128i)) {
      _mm_stream_si128(f_dst, _mm_stream_load_si128(f_src));
      f_dst++;
      f_src++;
    }
  }

  void ERMSBmemcpy(void* dst, void* src, uint64_t size) {
    __asm__ __volatile__("rep movsb" : "+D"(dst), "+S"(src), "+c"(size) : : "memory");
  }
};

#endif  // PARTITIONER_H_
