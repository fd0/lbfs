
#ifndef LBFS_RANGES_H
#define LBFS_RANGES_H

#include "sfscd_prot.h"
#include "list.h"

struct range {
  uint64 start;
  uint64 len;
  list_entry<range> link;
  range(uint64 s, uint64 l) { start = s; len = l; }
};

class ranges {
private:
  list<range, &range::link> _l;
  uint64 _start;
  uint64 _size;

public:
  ranges(uint64 start, uint64 size)
    : _start(start), _size(size) {
    _l.insert_head(New range(0, 0));
  }
  ~ranges();
  void add(uint64, uint64);
  bool filled(uint64, uint64) const;
  bool has_next_gap(uint64, uint64&, uint64&) const;
};

#endif // __RANGES_H
