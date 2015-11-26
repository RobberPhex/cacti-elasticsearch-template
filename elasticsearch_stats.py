#!/usr/bin/env python3

import argparse
from urllib.request import urlopen
import json

path_to_key = {
    'indices': {
        'docs': {
            'count': 'indeces_docs_count',
            'deleted': 'indeces_docs_deleted'
        },
        'store': {
            'size_in_bytes': 'indeces_store_size'
        },
        'indexing': {
            'index_total': 'indexing_index_total',
            'index_time_in_millis': 'indexing_index_time',
            'delete_total': 'indexing_delete_total',
            'delete_time_in_millis': 'indexing_delete_time'
        },
        'get': {
            'total': 'get_total',
            'time_in_millis': 'get_time',
            'exists_total': 'get_exists_total',
            'exists_time_in_millis': 'get_exists_time',
            'missing_total': 'get_missing_total',
            'missing_time_in_millis': 'get_missing_time'
        },
        'search': {
            'query_total': 'search_query_total',
            'query_time_in_millis': 'search_query_time',
            'fetch_total': 'search_fetch_total',
            'fetch_time_in_millis': 'search_fetch_time'
        },
        'merges': {
            'total': 'merges_total',
            'total_time_in_millis': 'merges_time',
            'total_docs': 'merges_total_docs',
            'total_size_in_bytes': 'merges_total_size'
        },
        'refresh': {
            'total': 'refresh_total',
            'total_time_in_millis': 'refresh_total_time'
        },
        'flush': {
            'total': 'flush_total',
            'total_time_in_millis': 'flush_total_time'
        },
        'warmer': {
            'total': 'warmer_total',
            'total_time_in_millis': 'warmer_total_time'
        },
        'fielddata': {
            'memory_size_in_bytes': 'fielddata_mem_size'
        },
        'completion': {
            'size_in_bytes': 'completion_size'
        },
        'segments': {
            'count': 'segments'
        }
    },
    'process': {
        'open_file_descriptors': 'process_open_files',
        'cpu': {
            'percent': 'process_cpu_percent',
            'total_in_millis': 'process_cpu_total'
        },
        'mem': {
            'total_virtual_in_bytes': 'process_mem_virtual'
        }
    },
    'jvm': {
        'uptime_in_millis': 'jvm_uptime',
        'mem': {
            'heap_used_in_bytes': 'jvm_mem_heap_used',
            'heap_committed_in_bytes': 'jvm_mem_heap_committed',
            'heap_max_in_bytes': 'jvm_mem_heap_max',
            'non_heap_used_in_bytes': 'jvm_mem_non_heap_used',
            'non_heap_committed_in_bytes': 'jvm_mem_non_heap_committed'
        },
        'threads': {
            'count': 'jvm_threads'
        }
    }
}


def get_by_all_path(data, path):
    for k, v in path.items():
        if isinstance(v, dict):
            get_by_all_path(data[k], v)
        else:
            print(v, data[k], sep=':', end=' ')


def main():
    parser = argparse.ArgumentParser(description='parse elasticsearch stats')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=9200)
    args = parser.parse_args()
    url = 'http://' + args.host + ':' + args.port + '/_nodes/stats'

    content = urlopen(url)
    content = json.loads(content.read().decode())
    node_id = list(content['nodes'])[0]
    node_stats = content['nodes'][node_id]
    get_by_all_path(node_stats, path_to_key)


if __name__ == "__main__":
    main()
