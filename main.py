from collections import defaultdict
import time
 
class QueryPlanAnalyser:
    def __init__(self):
        self.tables = {}
        self.indexes = {}
        self.stats = {}
 
    def add_table(self, name, n_rows, columns):
        self.tables[name] = {'rows': n_rows, 'columns': columns}
        self.stats[name] = {col: n_rows // 10 for col in columns}  # cardinality
 
    def add_index(self, table, column, index_type='btree'):
        if table not in self.indexes: self.indexes[table] = {}
        self.indexes[table][column] = index_type
 
    def estimate_cost(self, query):
        """Estimate query cost in I/O pages."""
        plan = self.parse_query(query)
        costs = {}
        for table in plan['tables']:
            if table not in self.tables: continue
            rows = self.tables[table]['rows']
            page_size = 100  # rows per page
            pages = rows // page_size
            if plan.get('filter_col') and table in self.indexes:
                if plan['filter_col'] in self.indexes[table]:
                    costs[table] = max(1, pages // 20)  # index scan
                    costs[table+'_type'] = 'INDEX_SCAN'
                else:
                    costs[table] = pages  # seq scan
                    costs[table+'_type'] = 'SEQ_SCAN'
            else:
                costs[table] = pages
                costs[table+'_type'] = 'SEQ_SCAN'
        return costs
 
    def parse_query(self, q):
        import re
        tables = re.findall(r'FROMs+(w+)|JOINs+(w+)', q, re.I)
        tables = [t for pair in tables for t in pair if t]
        filter_col = None
        m = re.search(r'WHEREs+w+.(w+)s*=', q, re.I)
        if m: filter_col = m.group(1)
        return {'tables': tables, 'filter_col': filter_col}
 
    def suggest_indexes(self, workload):
        suggestions = defaultdict(set)
        for query in workload:
            plan = self.parse_query(query)
            for t in plan['tables']:
                if plan.get('filter_col') and (t not in self.indexes or plan['filter_col'] not in self.indexes.get(t,{})):
                  suggestions[t].add(plan['filter_col'])
        return dict(suggestions)
 
analyser = QueryPlanAnalyser()
analyser.add_table('sales', 1000000, ['date','region','product','amount'])
analyser.add_table('products', 10000, ['id','category','price'])
analyser.add_index('sales', 'date')
analyser.add_index('products', 'id')
 
queries = [
    "SELECT SUM(amount) FROM sales WHERE sales.region = 'EAST' GROUP BY date",
    "SELECT * FROM products JOIN sales ON products.id = sales.product WHERE sales.date = '2024-01'",
]
for q in queries:
    costs = analyser.estimate_cost(q)
    print(f"Query: {q[:60]}")
    for k,v in costs.items():
        if not k.endswith('_type'): print(f"  {k}: {costs.get(k+'_type','?')} cost={v} pages")
 
suggestions = analyser.suggest_indexes(queries)
print(f"\nIndex suggestions: {suggestions}")
