import streamlit as st
import pandas as pd
from db.graph import GraphDB
from db.document import DocumentDB
import csv
from main import create_db, query1, query2

def main():
    st.title("Big Data Project 1: HetioNet DB Implementation")
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
        st.header("Drug Repurposing Analysis")
        st.markdown("""
        This analysis finds potential new drugs for a disease by:
        1. Identifying anatomical locations affected by the disease
        2. Finding genes that are up/down regulated in those locations
        3. Discovering drugs with opposite effects on those genes
        4. Excluding drugs that are already known treatments
        """)
        
        disease_id_q2 = st.text_input("Enter Disease ID", "Disease::DOID:1324")
        
        if st.button("Find Potential Drugs"):
            try:
                with st.spinner("Analyzing drug-gene-disease relationships..."):
                    drugs = query2(disease_id_q2, neo4j_uri)
                
                if drugs:
                    st.success(f"Found {len(drugs)} potential drugs!")
                    
                    # Create tabs for different views
                    results_tab1, results_tab2 = st.tabs(["📊 Analysis", "📋 Full Results"])
                    
                    with results_tab1:
                        # Show top 10 drugs in a bar chart
                        df_top10 = pd.DataFrame(drugs[:10], columns=["Drug"])
                        df_top10["Rank"] = range(1, len(df_top10) + 1)
                        
                        st.subheader("Top 10 Drug Candidates")
                        st.bar_chart(df_top10.set_index("Drug"))
                        
                        # Show top 10 in a nice formatted list
                        st.markdown("### Detailed Top 10")
                        for i, drug in enumerate(drugs[:10], 1):
                            st.markdown(f"**{i}. {drug}**")
                    
                    with results_tab2:
                        # Show full results in a searchable table
                        df_all = pd.DataFrame(drugs, columns=["Drug"])
                        st.dataframe(df_all)
                        
                        # Add download button
                        csv = df_all.to_csv(index=False)
                        st.download_button(
                            label="Download Complete Results",
                            data=csv,
                            file_name="potential_drugs.csv",
                            mime="text/csv"
                        )
                else:
                    st.warning("No potential drugs found for this disease")
                    st.info("""
                    This could mean:
                    - The disease ID doesn't exist
                    - No anatomical locations are associated
                    - No genes with opposite regulation patterns were found
                    """)
            except Exception as e:
                st.error(f"Error analyzing disease: {str(e)}")
                st.info("Please verify the disease ID and try again")

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