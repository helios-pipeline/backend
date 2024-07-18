Chose to prioritize other features for now, might come back later

# @api.route("/query", methods=["POST"])
# def query():
#     try:
#         client = current_app.get_ch_client()
#         print(f"in query route, client, from current_app.get_ch_client(), is: {client}")
#         # request: { query__string: 'show tables;',  }
#         # - Anything with a semicolon
#         # request: { query_string 'show tables', page: null, pageSize: null}
#         # - Shows page 1 of 0
#         # request: { query_string 'select * from events', page: null, pageSize: null}
#         # - Shows page 1 of 0

#         # request: { query_string: 'select * from information_schema.COLUMNS limit 100'}
#         # - Shows page 1 of 313
#         # - Not paginating by 10

#         # SELECT * FROM table LIMIT 100
#         # - row_count = 10
#         # - total_count = 100 (if table has 1000 rows total)

        
#         query_string, page, page_size, offset = destructure_query_request(request)
#         print('abc1', query_string, page, page_size, offset)
#         result = None
#         if not page or not page_size:
#           result = client.query(query_string)
#         else:
#           paginated_query = create_paginated_query(query_string, page_size, offset)
#           result = client.query(paginated_query)
            
#         is_show_statement = query_string.strip().upper().startswith("SHOW")
#         total_count = int(result.summary["read_rows"])
#         if not is_show_statement:
#             total_count_query = f"SELECT count(*) as total FROM ({query_string})"
#             count_result = client.query(total_count_query)  # Assume this function exists
#             total_count = int(count_result.result_rows[0][0])
#         if not page_size:
#             page_size = 10 
            
#         total_pages = ceil(total_count / page_size)
#         #total_count_query = f"SELECT count(*) as total FROM ({query_string})"
#         #total_count = client.query(total_count_query).first_row[0]
#         print('abc3')

#         data = [*result.named_results()]
#         print('abc4', len(result.result_rows))
#         response = {
#             "metadata": {
#                 "query": query_string,
#                 "row_count": int(result.summary["read_rows"]),
#                 "column_names": result.column_names,
#                 "column_types": [t.base_type for t in result.column_types],
#                 #"total_count": total_count,
#                 "page": page,
#                 "page_size": page_size,
#                 "total_pages": total_pages
#             },
#             "data": data,
#         }
#         return jsonify(response)
#     except Exception as e:
#         return jsonify({"Query Route Error": str(e)}), 400