import streamlit as st
import pandas as pd
from db.graph import GraphDB
from db.document import DocumentDB
import csv
from main import create_db, query1, query2

def main():
    st.title("HetioNet Database Explorer")
    st.sidebar.header("Configuration")

    # Database connection settings
    neo4j_uri = st.sidebar.text_input("Neo4j URI", "bolt://localhost:7687")
    mongo_uri = st.sidebar.text_input("MongoDB URI", "mongodb://localhost:27017/")

    # Main area tabs
    tab1, tab2, tab3 = st.tabs(["Database Creation", "Query 1", "Query 2"])

    # Database Creation Tab
    with tab1:
        st.header("Create Database")
        nodes_file = st.file_uploader("Upload Nodes TSV", type=['tsv'])
        edges_file = st.file_uploader("Upload Edges TSV", type=['tsv'])
        
        if st.button("Create Database") and nodes_file and edges_file:
            try:
                # Save uploaded files temporarily
                with open("temp_nodes.tsv", "wb") as f:
                    f.write(nodes_file.getvalue())
                with open("temp_edges.tsv", "wb") as f:
                    f.write(edges_file.getvalue())
                
                with st.spinner("Creating database..."):
                    create_db("temp_nodes.tsv", "temp_edges.tsv", neo4j_uri, mongo_uri)
                st.success("Database created successfully!")
                
                # Preview the data
                df_nodes = pd.read_csv(nodes_file, sep='\t')
                df_edges = pd.read_csv(edges_file, sep='\t')
                
                st.subheader("Nodes Preview")
                st.dataframe(df_nodes.head())
                st.subheader("Edges Preview")
                st.dataframe(df_edges.head())
            except Exception as e:
                st.error(f"Error creating database: {str(e)}")

    # Query 1 Tab
    with tab2:
        st.header("Disease Information Query")
        disease_id = st.text_input("Enter Disease ID", "Disease::DOID:2377")
        
        if st.button("Run Query 1"):
            try:
                with st.spinner("Querying database..."):
                    result = query1(disease_id, neo4j_uri)
                if result:
                    st.subheader("Disease Information")
                    st.write("Disease Name:", result["disease_name"])
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("Drug Names:")
                        st.write(result["drug_names"])
                        
                        st.write("Gene Names:")
                        st.write(result["gene_names"])
                    
                    with col2:
                        st.write("Locations:")
                        st.write(result["locations"])
                else:
                    st.warning("No results found for this disease ID")
            except Exception as e:
                st.error(f"Error executing query: {str(e)}")

    # Query 2 Tab
    with tab3:
        st.header("Potential Drugs Query")
        disease_id_q2 = st.text_input("Enter Disease ID for Drug Query", "Disease::DOID:2377")
        
        if st.button("Run Query 2"):
            try:
                with st.spinner("Finding potential drugs..."):
                    drugs = query2(disease_id_q2, neo4j_uri)
                if drugs:
                    st.subheader("Potential Drugs")
                    st.write(drugs)
                else:
                    st.warning("No potential drugs found for this disease")
            except Exception as e:
                st.error(f"Error executing query: {str(e)}")

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "This application allows you to explore the HetioNet database "
        "through an interactive interface. You can create the database "
        "from TSV files and run various queries to explore relationships "
        "between diseases, drugs, genes, and anatomical locations."
    )

if __name__ == "__main__":
    main()