
#include "ranges.h"

ranges::~ranges()
{
  range *p, *np;
  for(p = _l.first; p; p = np){
    np = (p->link).next;
    delete p;
  }
}

void
ranges::add(uint64 start, uint64 len)
{
  // look at each gap. _l guaranteed to start w/ an entry at zero.
  range *p, *np;
  for (p = _l.first; p; p = np) {
    np = (p->link).next;
    assert(np == 0 || np->start >= p->start + p->len);
    uint64 gs = p->start + p->len;
    uint64 ge = (np ? np->start : _size);
    if (gs != ge && start+len >= gs && start < ge) {
      uint64 ns = max(gs, start);
      uint64 ne = min(ge, start+len);
      range *r = New range(ns, ne - ns);
      r->link.next = np;
      r->link.pprev = &(p->link.next);
      p->link.next = r;
      if(np)
        np->link.pprev = &(r->link.next);
    }
  }
}

// is the entire start/len interval filled in?
bool
ranges::filled(uint64 start, uint64 len) const
{
  // does any gap overlap with start/end?
  range *p, *np;
  for (p = _l.first; p; p = np) {
    np = (p->link).next;
    uint64 gs = p->start + p->len;
    uint64 ge = (np ? np->start : _size);
    if (start+len > gs && start < ge && gs != ge)
      return false;
  }
  return true;
}

bool
ranges::has_next_gap(uint64 off, uint64 &start, uint64 &size) const
{
  range *p, *np;

  for (p = _l.first; p; p = np) {
    np = (p->link).next;
    uint64 gs = p->start + p->len;
    uint64 ge = (np ? np->start : _size);
    if (gs != ge && gs >= off) {
      start = gs;
      size = ge - gs;
      return true;
    }
  }
  return false;
}

