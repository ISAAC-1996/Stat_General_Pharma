################# Connexion #############
#Library
library(mailR)
library(jsonlite)
library(DBI)
library(odbc)
library(plotly)
library(tidyverse)
library(ggplot2)
library(mailR)
library(openxlsx)
library(gganimate)
library(ggforce)
library(lubridate)
library(taskscheduleR)
library(writexl)
library(httr)
library(purrr)
library(magrittr)
library(rmarkdown)
library(pagedown)
library(jsonlite)
library(base64enc)
library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
#CONNEXION A LA BASE DE DONNEE SQL Server
con <- DBI::dbConnect(odbc::odbc(), Database = "OSP_DATASTAT", 
                      Driver = Sys.getenv("sql_driver"), Server = Sys.getenv("sql_server"), 
                      UID = Sys.getenv("sql_uid"), PWD = Sys.getenv("sql_pwd"), 
                      Port = Sys.getenv("sql_port"))


              ############ Date R ##########
mois_precedent <- function(format = "texte") {
  date_courante <- Sys.Date()
  date_lt <- as.POSIXlt(date_courante)
  date_lt$mon <- date_lt$mon - 1
  date_mois_prec <- as.Date(date_lt)
  
  mois_fr <- c("Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
               "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre")
  
  annee_prec <- format(date_mois_prec, "%Y")
  mois_prec <- format(date_mois_prec, "%m")  # Format numérique 01-12
  
  if (format == "texte") {
    return(paste0(mois_fr[as.integer(mois_prec)], " ", annee_prec))  # Exemple: "Janvier 2025"
  } else if (format == "sql") {
    return(paste0(annee_prec, mois_prec))  # Exemple: "202501"
  } else {
    stop("Format non reconnu. Utilisez 'texte' ou 'sql'.")
  }
}
mois_precedent()




        ############ Fonction Nom et adresse des pharmacies ################
getPharmacieInfo <- function(n_auto_adhpha) {
  query <- "
    SELECT a.n_auto_adhpha,
           a.cip,
           a.rs_adhpha,
           a.cp_ville,
           a.nom_ville
    FROM vuProdAdhpha a
    LEFT JOIN os_ville v 
      ON v.n_auto_ville = a.ville_adhpha
    WHERE a.n_auto_adhpha = ?
  "
  df <- dbGetQuery(con, query, params = list(n_auto_adhpha))
  if (nrow(df) == 0) {
    warning("Aucune pharmacie trouvée pour n_auto_adhpha = ", n_auto_adhpha)
    return(NA_character_)
  }
  rs_adhpha_clean <- trimws(df$rs_adhpha)
  cp_clean        <- trimws(df$cp_ville)
  ville_clean     <- trimws(df$nom_ville)
  
  if (!grepl("(?i)\\pharmacie\\b", rs_adhpha_clean)) {
    rs_adhpha_clean <- paste0("Pharmacie ", rs_adhpha_clean)
  }
  
  result <- paste0(
    rs_adhpha_clean,
    " - ", cp_clean, "  ", ville_clean
  )
  return(result)
}

#getPharmacieInfo(9563)







########## Top 10 fuites géneriques ############
top_10_gener <- function(n_auto_adhpha_artic, con) {
  date_previous <- mois_precedent("sql")  
  periode <- date_previous  
  
  query <- sprintf("
select top 10 rtrim(concat(rtrim(libelle_court_groupe),' ',rtrim(conditionnement),' ',rtrim(volume),' ',rtrim(unite))) as Présentation, sum(qt_vendu_artic) as Quantité, sum(qt_vendu_artic*pfht) as PFHT
from (select lien as liensandoz, max(remise) as remsandoz
from os_labogener_artic where n_auto_adhfour=401884 and date_fin_application>getdate() and n_auto_artic  not in ( select n_auto_artic_ean from os_ean where code_ean in ( SELECT ean FROM osp_datastat.dbo.sandoz_exclus)) group by lien)t1 
inner join os_labogener_artic on liensandoz=lien inner join os_stat_artic on n_auto_artic=n_auto_artic_artic inner join os_grpgener_artic on os_labogener_artic.lien=os_grpgener_artic.lien  
left join os_adhfour ad on ad.n_auto_adhfour=os_labogener_artic.n_auto_adhfour where n_auto_adhpha_artic=%d
         and ((an_artic*100)+mois_artic) = %s
         and os_labogener_artic.n_auto_adhfour not in (401884) AND groupe_gener IN (select groupe_gener from os_gener inner join os_artic on n_auto_artic=n_auto_artic_gener where type_artic='G' and arret_artic is null) 
		 group by liensandoz, rtrim(concat(rtrim(libelle_court_groupe),' ',rtrim(conditionnement),' ',rtrim(volume),' ',rtrim(unite))), remsandoz, nom_adhfour, remise order by PFHT desc",
                   n_auto_adhpha_artic,
                   periode)
  result <- dbGetQuery(con, query)
  return(result)
}

########## Top 5 princeps non substitués ############

top_5 <- function(n_auto_adhpha_artic, con) {
  date_previous <- mois_precedent("sql")  
  periode <- date_previous  
  query <- sprintf("
 select top 5 rtrim(concat(rtrim(libelle_court_groupe),' ',rtrim(conditionnement),' ',rtrim(volume),' ',rtrim(unite))) as Présentation, nom_adhfour as Laboratoire, sum(qt_vendu_artic) as Quantité, sum(qt_vendu_artic*pfht_groupe) as PFHT
 from (select lien as liensandoz, max(remise) as remsandoz from os_labogener_artic where n_auto_adhfour=401884 and date_fin_application>getdate() and n_auto_artic  not in ( select n_auto_artic_ean from os_ean where code_ean in ( SELECT ean FROM osp_datastat.dbo.sandoz_exclus)) group by lien)t1 
 inner join os_labogener_artic on liensandoz=lien inner join os_stat_artic on n_auto_artic=n_auto_artic_artic inner join os_grpgener_artic on os_labogener_artic.lien=os_grpgener_artic.lien inner join os_artic on os_artic.n_auto_artic=n_auto_artic_artic inner join os_adhfour on os_adhfour.n_auto_adhfour=os_artic.adhfour 
 where n_auto_adhpha_artic= %d
         and ((an_artic*100)+mois_artic) = %s
         and os_labogener_artic.type_artic='P' group by liensandoz, rtrim(concat(rtrim(libelle_court_groupe),' ',rtrim(conditionnement),' ',rtrim(volume),' ',rtrim(unite))), remsandoz, nom_adhfour
		 order by PFHT desc",
                   n_auto_adhpha_artic,
                   periode)
  result <- dbGetQuery(con, query)
  return(result)
}

##################### Lancement sur les 12 derniers mois #####################
get_period_range <- function() {
  end_period <- mois_precedent("sql")  # Ex : "202507"
  
  # Sépare l'année et le mois
  end_year  <- as.integer(substr(end_period, 1, 4))
  end_month <- as.integer(substr(end_period, 5, 6))
  start_year  <- end_year
  start_month <- end_month - 5
  if (start_month <= 0) {
    start_month <- start_month + 12
    start_year  <- start_year - 1
  }

  start_period <- sprintf("%d%02d", start_year, start_month)
  
  return(list(
    start = start_period,
    end   = end_period
  ))
}
get_period_range()

# --------------- Depuis Mars 2025 ------------

depuis_Mars <- function() {
  start_period <- "202503"
  end_period <- mois_precedent("sql")
  
  # Extraire l'année et le mois de fin
  end_year <- as.integer(substr(end_period, 1, 4))
  end_month <- as.integer(substr(end_period, 5, 6))
  start_year <- 2025
  start_month <- 3
  if (end_year < start_year || (end_year == start_year && end_month < start_month)) {
    stop("La période n'est pas valide : le mois actuel est antérieur à mars 2025")
  }
  
  return(list(
    start = start_period,
    end = end_period
  ))
}
depuis_Mars()



# Fonction principale pour calculer les lancements
calculer_lancements <- function(n_auto_adhpha_artic, con) {
  # Obtenir la période des 12 derniers mois
  period <- get_period_range()
  query <- sprintf("
    SELECT top 10
      dci as Lancements, 
    COALESCE(SANDOZ * 10000 / NULLIF(TOTAL_GX, 0) * 0.01, 0) AS 'PDM Gx Sandoz',
    COALESCE(SANDOZ, 0) AS Sandoz,
    COALESCE(CONCURRENT, 0) AS 'Autres génériqueurs',
    COALESCE(PRINCEPS, 0) AS Princeps
    FROM (
      SELECT DISTINCT lib_dci as dci 
      FROM os_dci 
      WHERE groupe_dci IN (
        SELECT groupe_gener 
        FROM os_gener 
        WHERE n_auto_artic_gener IN (
          SELECT n_auto_artic_ean 
          FROM os_ean 
          WHERE code_ean IN (SELECT ean FROM sandoz_lancements)
        )
      )
    ) t1 
    LEFT JOIN (
      SELECT 
        lib_dci,
        SUM(CASE WHEN adhfour=401884 THEN qt_vendu_artic ELSE 0 END) as SANDOZ,
        SUM(CASE WHEN adhfour NOT IN (401884) AND type_artic IN ('G','A') THEN qt_vendu_artic ELSE 0 END) as CONCURRENT,
        NULLIF(SUM(CASE WHEN type_artic IN ('G', 'A') THEN qt_vendu_artic ELSE 0 END), 0) as TOTAL_GX,
        SUM(CASE WHEN type_artic='P' THEN qt_vendu_artic ELSE 0 END) as PRINCEPS
      FROM os_stat_artic
      INNER JOIN os_gener ON n_auto_artic_artic=n_auto_artic_gener
      INNER JOIN os_dci ON groupe_gener=groupe_dci
      INNER JOIN os_artic ON n_auto_artic_artic=n_auto_artic
      WHERE n_auto_adhpha_artic=%d
      AND n_auto_artic_artic IN (
        SELECT n_auto_artic_ean 
        FROM os_ean 
        WHERE code_ean IN (SELECT ean FROM sandoz_lancements)
      )
      AND periode BETWEEN %s AND %s
      GROUP BY lib_dci
    ) t2 ON dci=t2.lib_dci
    ORDER BY 'PDM Gx Sandoz' DESC",
                   n_auto_adhpha_artic,
                   period$start,
                   period$end
  )
  result <- dbGetQuery(con, query)
  return(result)
}

########## Sommes des switch ###############
# Fonction pour calculer la somme des switchs
Somme_switch <- function(n_auto_adhpha_artic, con) {
  # Obtenir le mois précédent au format YYYYMM
  date_previous <- mois_precedent("sql")
  
  # Construction de la requête SQL
  query <- sprintf("
    SELECT COALESCE(SUM(sellout), 0) as total_switch
    FROM (
      SELECT SUM(qt_vendu_artic*pfht) as sellout
      FROM (
        SELECT lien as liensandoz, MAX(remise) as remsandoz 
        FROM os_labogener_artic 
        WHERE n_auto_adhfour=401884 
        AND date_fin_application > GETDATE() and n_auto_artic  not in ( select n_auto_artic_ean from os_ean where code_ean in ( SELECT ean FROM osp_datastat.dbo.sandoz_exclus))
        GROUP BY lien
      ) t1 
      INNER JOIN os_labogener_artic ON liensandoz = lien 
      INNER JOIN os_stat_artic ON n_auto_artic = n_auto_artic_artic 
      INNER JOIN os_grpgener_artic ON os_labogener_artic.lien = os_grpgener_artic.lien  
      LEFT JOIN os_adhfour ad ON ad.n_auto_adhfour = os_labogener_artic.n_auto_adhfour 
      WHERE n_auto_adhpha_artic = %d
      AND ((an_artic*100) + mois_artic) = %s
      AND os_labogener_artic.n_auto_adhfour NOT IN (401884) 
      AND groupe_gener IN (
        SELECT groupe_gener 
        FROM os_gener 
        INNER JOIN os_artic ON n_auto_artic = n_auto_artic_gener 
        WHERE type_artic = 'G' 
        AND arret_artic IS NULL
      )
      GROUP BY liensandoz, 
               RTRIM(CONCAT(RTRIM(libelle_court_groupe), ' ', 
                           RTRIM(conditionnement), ' ',
                           RTRIM(volume), ' ',
                           RTRIM(unite))), 
               remsandoz, 
               nom_adhfour, 
               remise
    ) subquery",
                   n_auto_adhpha_artic,
                   date_previous
  )
  
  # Exécution de la requête
  result <- dbGetQuery(con, query)
  
  # Ajout d'attributs pour la traçabilité
  attr(result, "periode") <- mois_precedent("texte")
  attr(result, "pharmacie_id") <- n_auto_adhpha_artic
  
  return(result$total_switch)
}
#somme <- Somme_switch(12885, con)
#somme

############## Sommes des princeps #################

# Fonction pour calculer la somme des princeps
Somme_princeps <- function(n_auto_adhpha_artic, con) {
  date_previous <- mois_precedent("sql")
  query <- sprintf("
    SELECT COALESCE(SUM(sellout), 0) as total_princeps
    FROM (
      SELECT 
        liensandoz,
        RTRIM(CONCAT(RTRIM(libelle_court_groupe),' ',
                    RTRIM(conditionnement),' ',
                    RTRIM(volume),' ',
                    RTRIM(unite))) as name,
        SUM(qt_vendu_artic * pfht_groupe) as sellout,
        SUM(qt_vendu_artic) as quantity,
        remsandoz,
        nom_adhfour as manufacturer
      FROM (
        SELECT 
          lien as liensandoz,
          MAX(remise) as remsandoz 
        FROM os_labogener_artic 
        WHERE n_auto_adhfour = 401884 
        AND date_fin_application > GETDATE() and n_auto_artic  not in ( select n_auto_artic_ean from os_ean where code_ean in ( SELECT ean FROM osp_datastat.dbo.sandoz_exclus))
        GROUP BY lien
      ) t1 
      INNER JOIN os_labogener_artic ON liensandoz = lien 
      INNER JOIN os_stat_artic ON n_auto_artic = n_auto_artic_artic 
      INNER JOIN os_grpgener_artic ON os_labogener_artic.lien = os_grpgener_artic.lien 
      INNER JOIN os_artic ON os_artic.n_auto_artic = n_auto_artic_artic 
      INNER JOIN os_adhfour ON os_adhfour.n_auto_adhfour = os_artic.adhfour 
      WHERE n_auto_adhpha_artic = %d
      AND ((an_artic*100) + mois_artic) = %s
      AND os_labogener_artic.type_artic = 'P'
      GROUP BY 
        liensandoz,
        RTRIM(CONCAT(RTRIM(libelle_court_groupe),' ',
                    RTRIM(conditionnement),' ',
                    RTRIM(volume),' ',
                    RTRIM(unite))),
        remsandoz,
        nom_adhfour
    ) subquery",
                   n_auto_adhpha_artic,
                   date_previous
  )
  
  result <- dbGetQuery(con, query)
  attr(result, "periode") <- mois_precedent("texte")
  attr(result, "pharmacie_id") <- n_auto_adhpha_artic
  
  return(result$total_princeps)
}

# Exemple d'utilisation :
#somme_p <- Somme_princeps(12885, con)
#somme_p






################### Biosimilaire : une seule ligne par molécule,
Biosimilaire <- function(n_auto_adhpha_ecollect, con) {
  date_previous <- mois_precedent("sql")
  query <- sprintf("WITH 
BaseMols AS (
  SELECT 'FILGRASTIM'    AS base_mol
  UNION ALL SELECT 'PEGFILGRASTIM'
  UNION ALL SELECT 'EPOETINE'
  UNION ALL SELECT 'ETANERCEPT'
  UNION ALL SELECT 'ADALIMUMAB'
  UNION ALL SELECT 'ENOXAPARINE'
),
Ventes AS (
  SELECT
    CASE WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%FILGRASTIM%%'
           AND concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) NOT LIKE '%%PEG%%'
        THEN 'FILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%PEGFILGRASTIM%%'
        THEN 'PEGFILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%EPOETINE%%'
        THEN 'EPOETINE'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ETANERCEPT%%'
        THEN 'ETANERCEPT'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ADALIMUMAB%%'
        THEN 'ADALIMUMAB'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ENOXAPARINE%%'
        THEN 'ENOXAPARINE'
      ELSE NULL
    END AS base_mol,
    CASE WHEN adhfour=401884 THEN 'SANDOZ' ELSE 'AUTRES' END AS labo,
    SUM(qt_vendu_lcollect)              AS Qte,
    SUM(qt_vendu_lcollect * P_Fab_HT)   AS PFHT
  FROM v_el_collect_ospharm
  INNER JOIN os_gener   ON n_auto_artic_lcollect = n_auto_artic_gener
  INNER JOIN os_dci     ON groupe_gener = groupe_dci
  INNER JOIN os_artic   ON n_auto_artic_lcollect = n_auto_artic
  INNER JOIN os_adhfour ON adhfour = n_auto_adhfour
  LEFT JOIN OSP_PROD.dbo.CEPS_Prix 
         ON n_auto_artic_lcollect = id_sql 
        AND actif = 1
  WHERE n_auto_adhpha_ecollect = %d
    AND type_artic IN ('R','B')
    AND groupe_dci IN (
      SELECT groupe_gener 
      FROM os_grpgener_artic 
      WHERE libelle_court_groupe LIKE 'filgrastim%%'
         OR libelle_court_groupe LIKE 'pegfilgrastim%%'
         OR libelle_court_groupe LIKE 'epoetine%%'
         OR libelle_court_groupe LIKE 'etanercept%%'
         OR libelle_court_groupe LIKE 'adalimumab%%'
         OR libelle_court_groupe LIKE 'enoxaparine%%') AND periode = %s
  GROUP BY
    CASE
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%FILGRASTIM%%'
           AND concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) NOT LIKE '%%PEG%%'
        THEN 'FILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%PEGFILGRASTIM%%'
        THEN 'PEGFILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%EPOETINE%%'
        THEN 'EPOETINE'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ETANERCEPT%%'
        THEN 'ETANERCEPT'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ADALIMUMAB%%'
        THEN 'ADALIMUMAB'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ENOXAPARINE%%'
        THEN 'ENOXAPARINE'
      ELSE NULL
    END,
    CASE WHEN adhfour=401884 THEN 'SANDOZ' ELSE 'AUTRES' END
),
Pivoted AS (
  SELECT
    v.base_mol,
    SUM(CASE WHEN v.labo='SANDOZ' THEN v.Qte  ELSE 0 END) AS QteSandoz,
    SUM(CASE WHEN v.labo='SANDOZ' THEN v.PFHT ELSE 0 END)AS PFHTSandoz,
    SUM(CASE WHEN v.labo='AUTRES' THEN v.Qte  ELSE 0 END) AS QteAutres,
    SUM(CASE WHEN v.labo='AUTRES' THEN v.PFHT ELSE 0 END)AS PFHTAutres
  FROM Ventes v
  WHERE v.base_mol IS NOT NULL  -- on ignore tout ce qui n'est pas dans nos 6 molécules
  GROUP BY v.base_mol),
ResultsWithOrder AS (
  SELECT
    CASE b.base_mol
      WHEN 'FILGRASTIM'    THEN 'FILGRASTIM (ZARZIO)'
      WHEN 'PEGFILGRASTIM' THEN 'PEGFILGRASTIM (ZIEXTENZO)'
      WHEN 'EPOETINE'      THEN 'EPOETINE (BINOCRIT)'
      WHEN 'ETANERCEPT'    THEN 'ETANERCEPT (ERELZI)'
      WHEN 'ADALIMUMAB'    THEN 'ADALIMUMAB (HYRIMOZ)'
      WHEN 'ENOXAPARINE'   THEN 'ENOXAPARINE (ENOXAPARINE BECAT)'
      ELSE 'INCONNU'
    END AS [Molécules],
    COALESCE(p.QteSandoz,   0) AS [Quantité Sandoz],
    COALESCE(p.QteAutres,   0) AS [Quantité autres laboratoires],
    COALESCE(p.PFHTSandoz,  0) AS [PFHT Sandoz],
    COALESCE(p.PFHTAutres,  0) AS [PFHT autres laboratoires],
    CAST(CASE WHEN COALESCE(p.PFHTSandoz,0) + COALESCE(p.PFHTAutres,0) = 0 THEN 0
        ELSE ROUND(
          100.0 * COALESCE(p.PFHTSandoz,0)/ (COALESCE(p.PFHTSandoz,0)+COALESCE(p.PFHTAutres,0)),2)END AS DECIMAL(10,2)) AS [PDM PFHT],
    0 AS [Order],
    CASE 
      WHEN b.base_mol='FILGRASTIM'     THEN 1
      WHEN b.base_mol='PEGFILGRASTIM'  THEN 2
      WHEN b.base_mol='EPOETINE'       THEN 3
      WHEN b.base_mol='ETANERCEPT'     THEN 4
      WHEN b.base_mol='ADALIMUMAB'     THEN 5
      WHEN b.base_mol='ENOXAPARINE'    THEN 6
      ELSE 999 
    END AS [SortOrder]
  FROM BaseMols b
  LEFT JOIN Pivoted p ON p.base_mol = b.base_mol
  UNION ALL
  SELECT
    'TOTAL' AS [Molécules],
    SUM(COALESCE(p.QteSandoz, 0)) AS [Quantité Sandoz],
    SUM(COALESCE(p.QteAutres, 0)) AS [Quantité autres laboratoires],
    SUM(COALESCE(p.PFHTSandoz, 0)) AS [PFHT Sandoz],
    SUM(COALESCE(p.PFHTAutres, 0)) AS [PFHT autres laboratoires],
    CAST(CASE WHEN SUM(COALESCE(p.PFHTSandoz,0)) + SUM(COALESCE(p.PFHTAutres,0)) = 0 THEN 0
        ELSE ROUND(
          100.0 * SUM(COALESCE(p.PFHTSandoz,0))/ (SUM(COALESCE(p.PFHTSandoz,0))+SUM(COALESCE(p.PFHTAutres,0))),2)END AS DECIMAL(10,2)) AS [PDM PFHT],
    1 AS [Order],
    999 AS [SortOrder]
  FROM BaseMols b
  LEFT JOIN Pivoted p ON p.base_mol = b.base_mol
)
SELECT 
  [Molécules],
  [Quantité Sandoz],
  [Quantité autres laboratoires],
  [PFHT Sandoz],
  [PFHT autres laboratoires],
  [PDM PFHT]
FROM ResultsWithOrder
ORDER BY [Order], [SortOrder]
",
  n_auto_adhpha_ecollect,
  date_previous
  )

# 3) Exécution
result <- DBI::dbGetQuery(con, query)
return(result)
}

# Exécuter la fonction
#res_bio <- Biosimilaire(178, con)
#print(res_bio)


#------------------------ Biosimilaire : une seule ligne par molécule --------------------------
Biosimilaire_D_Mars <- function(n_auto_adhpha_ecollect, con) {
  period <- depuis_Mars()
  query <- sprintf("WITH 
BaseMols AS (
  SELECT 'FILGRASTIM'    AS base_mol
  UNION ALL SELECT 'PEGFILGRASTIM'
  UNION ALL SELECT 'EPOETINE'
  UNION ALL SELECT 'ETANERCEPT'
  UNION ALL SELECT 'ADALIMUMAB'
  UNION ALL SELECT 'ENOXAPARINE'
),
Ventes AS (
  SELECT
    CASE WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%FILGRASTIM%%'
           AND concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) NOT LIKE '%%PEG%%'
        THEN 'FILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%PEGFILGRASTIM%%'
        THEN 'PEGFILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%EPOETINE%%'
        THEN 'EPOETINE'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ETANERCEPT%%'
        THEN 'ETANERCEPT'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ADALIMUMAB%%'
        THEN 'ADALIMUMAB'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ENOXAPARINE%%'
        THEN 'ENOXAPARINE'
      ELSE NULL
    END AS base_mol,
    CASE WHEN adhfour=401884 THEN 'SANDOZ' ELSE 'AUTRES' END AS labo,
    SUM(qt_vendu_lcollect)              AS Qte,
    SUM(qt_vendu_lcollect * P_Fab_HT)   AS PFHT
  FROM v_el_collect_ospharm
  INNER JOIN os_gener   ON n_auto_artic_lcollect = n_auto_artic_gener
  INNER JOIN os_dci     ON groupe_gener = groupe_dci
  INNER JOIN os_artic   ON n_auto_artic_lcollect = n_auto_artic
  INNER JOIN os_adhfour ON adhfour = n_auto_adhfour
  LEFT JOIN OSP_PROD.dbo.CEPS_Prix 
         ON n_auto_artic_lcollect = id_sql 
        AND actif = 1
  WHERE n_auto_adhpha_ecollect =%d
    AND type_artic IN ('R','B')
    AND groupe_dci IN (
      SELECT groupe_gener 
      FROM os_grpgener_artic 
      WHERE libelle_court_groupe LIKE 'filgrastim%%'
         OR libelle_court_groupe LIKE 'pegfilgrastim%%'
         OR libelle_court_groupe LIKE 'epoetine%%'
         OR libelle_court_groupe LIKE 'etanercept%%'
         OR libelle_court_groupe LIKE 'adalimumab%%'
         OR libelle_court_groupe LIKE 'enoxaparine%%') AND periode between %s AND %s
  GROUP BY
    CASE
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%FILGRASTIM%%'
           AND concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) NOT LIKE '%%PEG%%'
        THEN 'FILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%PEGFILGRASTIM%%'
        THEN 'PEGFILGRASTIM'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%EPOETINE%%'
        THEN 'EPOETINE'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ETANERCEPT%%'
        THEN 'ETANERCEPT'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ADALIMUMAB%%'
        THEN 'ADALIMUMAB'
      WHEN concat(rtrim(lib_dci),' ',rtrim(dosage_dci),' ',rtrim(forme_dci)) LIKE '%%ENOXAPARINE%%'
        THEN 'ENOXAPARINE'
      ELSE NULL
    END,
    CASE WHEN adhfour=401884 THEN 'SANDOZ' ELSE 'AUTRES' END
),
Pivoted AS (
  SELECT
    v.base_mol,
    SUM(CASE WHEN v.labo='SANDOZ' THEN v.Qte  ELSE 0 END) AS QteSandoz,
    SUM(CASE WHEN v.labo='SANDOZ' THEN v.PFHT ELSE 0 END)AS PFHTSandoz,
    SUM(CASE WHEN v.labo='AUTRES' THEN v.Qte  ELSE 0 END) AS QteAutres,
    SUM(CASE WHEN v.labo='AUTRES' THEN v.PFHT ELSE 0 END)AS PFHTAutres
  FROM Ventes v
  WHERE v.base_mol IS NOT NULL  -- on ignore tout ce qui n'est pas dans nos 6 molécules
  GROUP BY v.base_mol),
ResultsWithOrder AS (
  SELECT
    CASE b.base_mol
      WHEN 'FILGRASTIM'    THEN 'FILGRASTIM (ZARZIO)'
      WHEN 'PEGFILGRASTIM' THEN 'PEGFILGRASTIM (ZIEXTENZO)'
      WHEN 'EPOETINE'      THEN 'EPOETINE (BINOCRIT)'
      WHEN 'ETANERCEPT'    THEN 'ETANERCEPT (ERELZI)'
      WHEN 'ADALIMUMAB'    THEN 'ADALIMUMAB (HYRIMOZ)'
      WHEN 'ENOXAPARINE'   THEN 'ENOXAPARINE (ENOXAPARINE BECAT)'
      ELSE 'INCONNU'
    END AS [Molécules],
    COALESCE(p.QteSandoz,   0) AS [Quantité Sandoz],
    COALESCE(p.QteAutres,   0) AS [Quantité autres laboratoires],
    COALESCE(p.PFHTSandoz,  0) AS [PFHT Sandoz],
    COALESCE(p.PFHTAutres,  0) AS [PFHT autres laboratoires],
    CAST(CASE WHEN COALESCE(p.PFHTSandoz,0) + COALESCE(p.PFHTAutres,0) = 0 THEN 0
        ELSE ROUND(
          100.0 * COALESCE(p.PFHTSandoz,0)/ (COALESCE(p.PFHTSandoz,0)+COALESCE(p.PFHTAutres,0)),2)END AS DECIMAL(10,2)) AS [PDM PFHT],
    0 AS [Order],
    CASE 
      WHEN b.base_mol='FILGRASTIM'     THEN 1
      WHEN b.base_mol='PEGFILGRASTIM'  THEN 2
      WHEN b.base_mol='EPOETINE'       THEN 3
      WHEN b.base_mol='ETANERCEPT'     THEN 4
      WHEN b.base_mol='ADALIMUMAB'     THEN 5
      WHEN b.base_mol='ENOXAPARINE'    THEN 6
      ELSE 999 
    END AS [SortOrder]
  FROM BaseMols b
  LEFT JOIN Pivoted p ON p.base_mol = b.base_mol
  UNION ALL
  SELECT
    'TOTAL' AS [Molécules],
    SUM(COALESCE(p.QteSandoz, 0)) AS [Quantité Sandoz],
    SUM(COALESCE(p.QteAutres, 0)) AS [Quantité autres laboratoires],
    SUM(COALESCE(p.PFHTSandoz, 0)) AS [PFHT Sandoz],
    SUM(COALESCE(p.PFHTAutres, 0)) AS [PFHT autres laboratoires],
    CAST(CASE WHEN SUM(COALESCE(p.PFHTSandoz,0)) + SUM(COALESCE(p.PFHTAutres,0)) = 0 THEN 0
        ELSE ROUND(
          100.0 * SUM(COALESCE(p.PFHTSandoz,0))/ (SUM(COALESCE(p.PFHTSandoz,0))+SUM(COALESCE(p.PFHTAutres,0))),2)END AS DECIMAL(10,2)) AS [PDM PFHT],
    1 AS [Order],
    999 AS [SortOrder]
  FROM BaseMols b
  LEFT JOIN Pivoted p ON p.base_mol = b.base_mol
)
SELECT 
  [Molécules],
  [Quantité Sandoz],
  [Quantité autres laboratoires],
  [PFHT Sandoz],
  [PFHT autres laboratoires],
  [PDM PFHT]
FROM ResultsWithOrder
ORDER BY [Order], [SortOrder]",n_auto_adhpha_ecollect,
  period$start,
  period$end
  )
#Exécution
result <- DBI::dbGetQuery(con, query)
return(result)
}
# Exécuter la fonction
#res_bio2 <- Biosimilaire_D_Mars(1828, con)
#print(res_bio2)


 ############## TRONQUER DES TEXTE  ###########
truncate_string <- function(txt, max_len = 50) {
  if (nchar(txt) > max_len) {
    paste0(substr(txt, 1, max_len), " ")
  } else {
    txt
  }
}


### Premiere lettre en Majuscule  ########

capitalize_first <- function(txt) {
  if (nchar(txt) == 0) return(txt)
  txt_lower <- tolower(txt)
  first_char <- toupper(substr(txt_lower, 1, 1))
  rest_chars <- substr(txt_lower, 2, nchar(txt_lower))
  paste0(first_char, rest_chars)
}

#### 
