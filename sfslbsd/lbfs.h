
#ifndef _LBFS_H_
#define _LBFS_H_

// for each chunk in the new_chunks list, fill in, in the same position in the
// reusable_chunks list, either 0L if the chunk fingerprint does not exist in
// the database, or a lbfs_chunk object if a chunk with the same fingerprint
// exists in the database. the reusable_chunks list is resized and modified by
// the function. objects on the reusbale_chunks list are allocated by the
// function, and therefore must be freed by the callee. return 0 if
// successful, -1 otherwise.
int
lbfs_search_reusable_chunks(vec<lbfs_chunk *> &new_chunks,
                            vec<lbfs_chunk_loc *> &reusable_chunks);

#endif _LBFS_H_

