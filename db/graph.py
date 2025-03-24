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
            result = session.run(
                """
                MATCH (d:Disease { id: $disease_id })
                OPTIONAL MATCH (d)<-[:TREATS|PALLIATES]-(c:Compound)
                OPTIONAL MATCH (d)-[:CAUSES]->(g:Gene)
                OPTIONAL MATCH (d)-[:OCCURS_IN|LOCALIZES_TO]->(l)
                RETURN d.name AS disease_name,
                       collect(DISTINCT c.name) AS drug_names,
                       collect(DISTINCT g.name) AS gene_names,
                       collect(DISTINCT l.name) AS locations
                """,
                disease_id=disease_id
            )
            record = result.single()
            return record

    def query2(self, new_disease_id):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (d:Disease { id: $disease_id })-[:OCCURS_IN|LOCALIZES_TO]->(loc)
                MATCH (loc)-[r1:UPREGULATES|DOWNREGULATES]->(g:Gene)
                MATCH (c:Compound)-[r2:UPREGULATES|DOWNREGULATES]->(g)
                WHERE
                  (
                    (type(r1) = "UPREGULATES" AND type(r2) = "DOWNREGULATES")
                    OR
                    (type(r1) = "DOWNREGULATES" AND type(r2) = "UPREGULATES")
                  )
                  AND NOT ((c)-[:TREATS]->(d))
                RETURN DISTINCT c.name AS potential_drug
                """,
                disease_id=new_disease_id
            )
            drugs = [record["potential_drug"] for record in result]
            return drugs
