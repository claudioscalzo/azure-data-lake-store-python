import Queue
from concurrent.futures import ThreadPoolExecutor
import multiprocessing


def recurse_method(AzureDLFileSystemObject=None, path=None, file_method=lambda: None, dir_method=lambda: None,
                   file_method_kwargs={}, dir_method_kwargs={}):
    """General purpose framework for traversing the dir tree and applying methods on files and dirs"""
    dir_queue = Queue.Queue()
    file_queue = Queue.Queue()
    dir_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()*3)
    file_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()*10)
    main_pool = ThreadPoolExecutor(max_workers=2)

    def threads_still_in_thread_pool(thread_pool):
        #TODO Is there a better way than accessing private members
        return len(thread_pool._threads) == 0 and thread_pool._work_queue.qsize() == 0

    def list_status(dir_path, ls_arguments={'detail': True}):
        #TODO Replace with Minimal API call Or more generic method call
        folders = AzureDLFileSystemObject.ls(dir_path, **ls_arguments)
        result = []
        for elements in folders:
            try:
                reduced_details = {key: elements[key] for key in ['type', 'pathSuffix']}
                result.append(reduced_details)
            except KeyError:
                #TODO Logging
                pass
        return result

    def dir_processor(dir_path):
        try:
            dir_method(path=dir_path, **dir_method_kwargs)
        except:
            # TODO Logging
            pass

        for x in list_status(dir_path=dir_path):
            if x['type'] == 'DIRECTORY':
                complete_path = dir_path + '/' + x['pathSuffix']
                dir_queue.put(complete_path)
            else:
                complete_path = dir_path + '/' + x['pathSuffix']
                file_queue.put(complete_path)

    def dir_thread():
        while not dir_queue.empty() or threads_still_in_thread_pool(dir_pool):
            if dir_queue.empty():
                continue
            else:
                dir_path = dir_queue.get()
                dir_pool.submit(dir_processor(dir_path))

    def file_processor(file_path):
        try:
            file_method(path=file_path, **file_method_kwargs)
        except:
            # TODO Logging
            pass

    def file_thread():
        while not file_queue.empty() or not dir_queue.empty() or threads_still_in_thread_pool(dir_pool):
            if file_queue.empty():
                continue
            else:
                file_path = file_queue.get()
                file_pool.submit(file_processor(file_path))

    dir_processor(path)
    main_pool.submit(dir_thread)
    main_pool.submit(file_thread)
