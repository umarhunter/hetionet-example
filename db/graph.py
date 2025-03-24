import re
from neo4j import GraphDatabase

class GraphDB:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_node(self, node_id, name, kind):
        # Allow labels that start with a letter and contain only letters, numbers, or underscores. 
        if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", kind):
            raise ValueError(f"Invalid kind label: {kind}")
        
        with self.driver.session() as session:
            # We insert the label directly using string formatting.
            query = (
                "MERGE (n { id: $id }) "
                "SET n.name = $name, n.kind = $kind, n:" + kind
            )
            session.run(query, id=node_id, name=name, kind=kind)

    def create_edge(self, source, metaedge, target):
        with self.driver.session() as session:
            # The relationship type is interpolated; ensure metaedge is safe as needed.
            query = (
                "MATCH (s { id: $source }) "
                "MATCH (t { id: $target }) "
                "MERGE (s)-[r:" + metaedge + "]->(t)"
            )
            session.run(query, source=source, target=target)

    def query1(self, disease_id):
        with self.driver.session() as session:
            # First verify the disease exists
            # this was the cause for many errors in the previous implementation
            verify = session.run("""
                MATCH (d:Disease {id: $disease_id}) 
                RETURN count(d) > 0 as exists
            """, disease_id=disease_id).single()
            
            if not verify or not verify["exists"]:
                print(f"Disease {disease_id} not found in database")
                return None

            # Main query using correct relationship types
            result = session.run("""
                MATCH (d:Disease {id: $disease_id})
                OPTIONAL MATCH (d)<-[r:CtD|CpD]-(c:Compound)
                OPTIONAL MATCH (d)-[r2:DdG]-(g:Gene)
                OPTIONAL MATCH (d)-[r3:DlA]->(l:Anatomy)
                RETURN d.name AS disease_name,
                    collect(DISTINCT c.name) AS drug_names,
                    collect(DISTINCT g.name) AS gene_names,
                    collect(DISTINCT l.name) AS locations
            """, disease_id=disease_id)
            
            record = result.single()
            if record:
                return {
                    "disease_name": record["disease_name"],
                    "drug_names": record["drug_names"],
                    "gene_names": record["gene_names"],
                    "locations": record["locations"]
                }
            return None

    def query2(self, new_disease_id):
        with self.driver.session() as session:
            # First get disease name
            disease = session.run("""
                MATCH (d:Disease {id: $disease_id})
                RETURN d.name as name
            """, disease_id=new_disease_id).single()

            print(f"\nAnalyzing drug repurposing for: {disease['name']}")
            
            # Get anatomical locations
            locations = session.run("""
                MATCH (d:Disease {id: $disease_id})-[:DlA]->(loc:Anatomy)
                RETURN count(loc) as count, collect(loc.name)[..5] as samples
            """, disease_id=new_disease_id).single()
            
            print(f"\nFound {locations['count']} affected anatomical locations")
            print(f"Sample locations: {', '.join(locations['samples'])}...")

            # Get gene regulation patterns
            gene_patterns = session.run("""
                MATCH (d:Disease {id: $disease_id})-[:DlA]->(loc:Anatomy)
                MATCH (loc)-[r:AuG|AdG]-(g:Gene)
                RETURN type(r) as regulation,
                    count(DISTINCT g) as gene_count
            """, disease_id=new_disease_id)
            
            print("\nGene regulation patterns:")
            for pattern in gene_patterns:
                print(f"- {pattern['regulation']}: {pattern['gene_count']} genes")

            # Main query for drug repurposing
            result = session.run("""
                MATCH (d:Disease {id: $disease_id})-[:DlA]->(loc:Anatomy)
                MATCH (loc)-[r1:AuG|AdG]-(g:Gene)
                MATCH (c:Compound)-[r2:CuG|CdG]->(g)
                WHERE 
                (
                    (type(r1) = 'AuG' AND type(r2) = 'CdG')
                    OR
                    (type(r1) = 'AdG' AND type(r2) = 'CuG')
                )
                AND NOT (c)-[:CtD]->(d)
                WITH DISTINCT c, 
                    count(DISTINCT g) as gene_count,
                    collect(DISTINCT g.name)[..3] as sample_genes
                RETURN c.name as drug_name,
                    gene_count,
                    sample_genes
                ORDER BY gene_count DESC
            """, disease_id=new_disease_id)

            drugs = []
            for record in result:
                drugs.append({
                    'name': record['drug_name'],
                    'gene_count': record['gene_count'],
                    'sample_genes': record['sample_genes']
                })

            if drugs:
                print(f"\nFound {len(drugs)} potential drugs")
                print("\nTop 5 candidates:")
                for drug in drugs[:5]:
                    print(f"- {drug['name']}")
                    print(f"  • Matches {drug['gene_count']} genes")
                    print(f"  • Sample affected genes: {', '.join(drug['sample_genes'])}")
            
            return [drug['name'] for drug in drugs]