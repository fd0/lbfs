
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
                            vec<lbfs_chunk *> &reusable_chunks);

// for each lbfs_chunk object in reusable_chunks list, load the chunk into
// new_fd at position specified by the corresponding lbfs_chunk object in
// new_chunks list. if the write is unsuccessful, the lbfs_chunk object in
// reusable_chunks list is deleted and the ptr in the list is set to 0.
// otherwise the object is untouched. returns the number of failed chunks.
// new_chunks list is untouched.
int
lbfs_load_reusable_chunks(int new_fd, 
                          vec<lbfs_chunk *> &new_chunks,
                          vec<lbfs_chunk *> &reusable_chunks);


// add a new file, currently named by tmppath, to the database and filesystem
// as file named by path: remove the file's old chunk information from the db,
// and insert new chunk information into the db. rename tmppath to oldpath.
// returns 0 if successful.
int
lbfs_add_file(const char *path, const char *tmppath);

#endif _LBFS_H_

