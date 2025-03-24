import csv
import argparse
import re
from db.graph import GraphDB
from db.document import DocumentDB
from tqdm import tqdm

def sanitize_metaedge(metaedge):
    # Remove all characters that are not letters, digits, or underscores.
    return re.sub(r'[^A-Za-z0-9_]', '', metaedge)

def create_db(nodes_file, edges_file, neo4j_uri, mongo_uri):
    """
    Creates and populates a graph database (Neo4j) and a document database (MongoDB) with nodes and edges.
    Args:
        nodes_file (str): Path to the file containing node data. The file should be in TSV format with columns 'id', 'name', and 'kind'.
        edges_file (str): Path to the file containing edge data. The file should be in TSV format with columns 'source', 'target', and 'metaedge'.
        neo4j_uri (str): URI for connecting to the Neo4j database.
        mongo_uri (str): URI for connecting to the MongoDB database.
    Returns:
        None
    """
    # Initialize both DB handlers
    graph_db = GraphDB(uri=neo4j_uri)
    doc_db = DocumentDB(uri=mongo_uri)
    
    # Load nodes
    with open(nodes_file, "r") as f:
        reader = csv.DictReader(f, delimiter='\t')
        nodes = list(reader)
    
    # Insert nodes into MongoDB with progress bar
    print("Inserting nodes into DocumentDB...")
    for i, row in tqdm(enumerate(nodes), total=len(nodes), desc="MongoDB Nodes"):
        node_doc = {
            "_id": row["id"],
            "name": row["name"],
            "kind": row["kind"]
        }
        doc_db.insert_node(node_doc)
    
    # Insert nodes into Neo4j with progress bar
    print("\nInserting nodes into GraphDB...")
    with graph_db.driver.session() as session:
        for row in tqdm(nodes, desc="Neo4j Nodes"):
            # 
            node_id = row["id"]
            name = row["name"]
            kind = row["kind"]
            session.run(
                "MERGE (n { id: $id }) "
                "SET n.name = $name, n.kind = $kind, n:" + kind,
                id=node_id, name=name, kind=kind
            )
    
    # Load edges
    with open(edges_file, "r") as f:
        reader = csv.DictReader(f, delimiter='\t') # since the file is in TSV format
        edges = list(reader)
    
    print("\nInserting edges into GraphDB (batched)...")


    # In theory, batching should help with performance since we're sending fewer queries to the database
    # Group edges by metaedge type
    edge_groups = {}
    for edge in edges:
        key = edge["metaedge"]
        edge_groups.setdefault(key, []).append({"source": edge["source"], "target": edge["target"]})
    
    batch_size = 1000
    with graph_db.driver.session() as session:
        for metaedge, edge_list in edge_groups.items():
            sanitized_metaedge = sanitize_metaedge(metaedge)
            for i in tqdm(range(0, len(edge_list), batch_size), desc=f"Edges for {metaedge}", leave=True):
                batch = edge_list[i:i+batch_size]
                query = (
                    "UNWIND $batch as row "
                    "MATCH (s { id: row.source }) "
                    "MATCH (t { id: row.target }) "
                    "MERGE (s)-[r:" + sanitized_metaedge + "]->(t)"
                )
                session.run(query, batch=batch)
    
    # Close DB connections
    graph_db.close()
    doc_db.close()
    print("\nDatabase creation complete.")



def query1(disease_id, neo4j_uri):
    """
    Given a disease id, this function retrieves the following information in a single query:
    - The name of the disease
    - The names of drugs that can treat or palliate this disease
    - The names of genes that cause this disease
    - The locations where this disease occurs

    Parameters:
    - disease_id (str): The ID of the disease to query.
    - neo4j_uri (str): The URI of the Neo4j database.

    Returns:
    - dict: A dictionary containing the queried information.
    """
    graph_db = GraphDB(uri=neo4j_uri)
    record = graph_db.query1(disease_id)
    graph_db.close()
    return record

def query2(new_disease_id, neo4j_uri):
    """
    Queries the graph database for drugs related to a specific disease.

    Paramaters:
        new_disease_id (str): The ID of the disease to query.
        neo4j_uri (str): The URI of the Neo4j database.

    Returns:
        list: A list of drugs related to the specified disease.
    """
    graph_db = GraphDB(uri=neo4j_uri)
    drugs = graph_db.query2(new_disease_id)
    graph_db.close()
    return drugs

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Big Data HetioNet Project")
    parser.add_argument("command", choices=["create", "query1", "query2"], help="Command to run")
    parser.add_argument("--diseaseId", help="Disease ID for queries")
    parser.add_argument("--nodes", default="data/nodes.tsv", help="Path to nodes TSV")
    parser.add_argument("--edges", default="data/edges.tsv", help="Path to edges TSV")
    parser.add_argument("--neo4jUri", default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--mongoUri", default="mongodb://localhost:27017/", help="MongoDB URI")

    args = parser.parse_args()

    if args.command == "create":
        create_db(args.nodes, args.edges, args.neo4jUri, args.mongoUri)
        print("Database creation complete.")
    elif args.command == "query1":
        if not args.diseaseId:
            print("Please provide --diseaseId for query1.")
        else:
            result = query1(args.diseaseId, args.neo4jUri)
            if result:
                print("Disease Name:", result["disease_name"])
                print("Drug Names:", result["drug_names"])
                print("Gene Names:", result["gene_names"])
                print("Locations:", result["locations"])
            else:
                print("No results found for disease ID:", args.diseaseId)
    elif args.command == "query2":
        if not args.diseaseId:
            print("Please provide --diseaseId for query2.")
        else:
            potential_drugs = query2(args.diseaseId, args.neo4jUri)
            print("Potential drugs:", potential_drugs)

if __name__ == "__main__":
    main()
