from os import scandir
from re import match


def get_target_dir_entries(root_dir_path, subdirs_rgx, file_rgx):
    """
    Build list of DirEntry objects below `root_dir_path` whose intervening subdirectory names match the `subdirs_rgx`
    regular expression and whose file name matches the `file_rgx` regular expression.

    :param root_dir_path: str path of root directory
    :param subdirs_rgx: str regular expression to match subdirectories
    :param file_rgx: str regular expression to match leaf files
    :return: list DirEntry objects
    """
    dir_entries_list = []
    root_dir_entries = scandir(root_dir_path)

    # loop over each DirEntry object
    for dir_entry in root_dir_entries:
        # if DirEntry object is a matching directory...
        if dir_entry.is_dir(follow_symlinks=False) and match(subdirs_rgx, dir_entry.name):
            # recurse down through the directory
            sub_dir_entries_list = get_target_dir_entries(dir_entry, subdirs_rgx, file_rgx)
            # append matching DirEntry objects list to `dir_entries_list`
            dir_entries_list = [*dir_entries_list, *sub_dir_entries_list]
        # else if DirEntry object is a matching file...
        elif dir_entry.is_file(follow_symlinks=False) and match(file_rgx, dir_entry.name):
            # append matching DirEntry object to `dir_entries_list`
            dir_entries_list.append(dir_entry)

    return dir_entries_list
