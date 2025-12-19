import os
import pandas as pd
import re
import config   



#-------------------------------------------------------------------------------------------------
# Fonction pour parcourir le dossier de sauvegarde et recréér le dataframe complet
#-------------------------------------------------------------------------------------------------
def process_all_pdf_files(output_dir=config.output_dir, final_output_name=config.final_output_name):
    """
    Parcourt le dossier extraction_files, lit les CSV et les combine dans l'ordre.
    Ignore le fichier global s'il existe déjà pour éviter la récursion lors de multiples exécutions.
    """
    if not os.path.exists(output_dir):
        print(f"❌ Le dossier {output_dir} n'existe pas.")
        return pd.DataFrame()

    # Lister tous les fichiers CSV
    files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    
    # Exclure le fichier de sortie s'il est déjà présent
    files = [f for f in files if final_output_name not in f]

    # Tri naturel (ex: page_2 avant page_10)
    # On extrait le premier nombre trouvé dans le nom du fichier
    def get_sort_key(filename):
        numbers = re.findall(r'\d+', filename)
        if numbers:
            return int(numbers[0])
        return 0
    
    files.sort(key=get_sort_key)
    
    print(f"\n🔄 Fusion de {len(files)} fichiers CSV trouvés dans '{output_dir}'...")
    
    all_dfs = []
    for filename in files:
        filepath = os.path.join(output_dir, filename)
        try:
            # Lecture avec le séparateur point-virgule utilisé à l'export
            df = pd.read_csv(filepath, sep=';')
            # Ajout d'une colonne source pour traçabilité (optionnel)
            # df['source_file'] = filename
            all_dfs.append(df)
            print(f"  - Chargé: {filename} ({len(df)} lignes)")
        except Exception as e:
            print(f"  ⚠️ Erreur lors de la lecture de {filename}: {e}")

    if not all_dfs:
        print("❌ Aucun fichier valide n'a été chargé.")
        return pd.DataFrame()

    # Concaténation
    full_df = pd.concat(all_dfs, ignore_index=True)
    
    # Export du résultat global
    output_csv = os.path.join(output_dir, f"{final_output_name}.csv")
    output_xlsx = os.path.join(output_dir, f"{final_output_name}.xlsx")
    
    print(f"\n💾 Sauvegarde du fichier global ({len(full_df)} lignes)...")
    
    full_df.to_csv(output_csv, index=False, sep=';', encoding='utf-8-sig')
    print(f"  ✅ CSV: {output_csv}")
    
    try:
        full_df.to_excel(output_xlsx, index=False)
        print(f"  ✅ Excel: {output_xlsx}")
    except ImportError:
        print("  ⚠️ Module openpyxl manquant pour l'export Excel.")
    except Exception as e:
        print(f"  ⚠️ Erreur export Excel: {e}")
        
    return full_df

from extract_table import extract_transactions_from_pdf, clean_and_format_dataframe, analyze_and_export, get_solde_precedent

def run_full_extraction(source_dir=config.input_dir, output_dir=config.output_dir):
    """
    Parcourt tous les PDF du dossier source, extrait les transactions
    et génère les fichiers CSV individuels.
    """
    if not os.path.exists(source_dir):
        print(f"❌ Dossier source introuvable: {source_dir}")
        return

    # Lister les fichiers PDF
    pdf_files = [f for f in os.listdir(source_dir) if f.lower().endswith(".pdf")]
    
    # Tri naturel (page_1, page_2, ..., page_10)
    pdf_files.sort(key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 0)
    
    print(f"\n🚀 Démarrage de l'extraction sur {len(pdf_files)} fichiers...")
    
    success_count = 0
    
    for filename in pdf_files:
        pdf_path = os.path.join(source_dir, filename)
        pdf_name = os.path.splitext(filename)[0]
        
        print(f"\n📄 Traitement de: {filename}")
        
        try:
            # 1. Tentative récupération solde (si présent sur la page)
            # Note: Souvent présent uniquement sur la première page ou en bas de page
            solde_prec = get_solde_precedent(pdf_path)
            
            # 2. Extraction
            df = extract_transactions_from_pdf(pdf_path)
            
            # 3. Nettoyage et Export
            if not df.empty:
                df_clean = clean_and_format_dataframe(df)
                # On exporte chaque page individuellement (pour debug et sécu)
                # Le nom du fichier PDF sert de préfixe
                analyze_and_export(df_clean, pdf_name, solde_prec)
                success_count += 1
            else:
                print(f"  ⚠️ Aucune transaction trouvée sur {filename}")
                
        except Exception as e:
            print(f"  ❌ Erreur sur {filename}: {e}")

    print(f"\n✨ Extraction terminée. {success_count}/{len(pdf_files)} fichiers traités avec succès.")

#-------------------------------------------------------------------------------------------------
# Main
#-------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. Lancer l'extraction de tous les PDF
    run_full_extraction(config.input_dir, config.output_dir)
    
    # 2. Fusionner tous les CSV générés
    process_all_pdf_files(config.output_dir, config.final_output_name)
