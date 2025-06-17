########################## Corps du mail et automatisation via R ###################
library(lubridate)  
library(future)
library(future.apply)

# Définir le chemin vers Pandoc - AJOUTER CES LIGNES ICI
#Sys.setenv(RSTUDIO_PANDOC = "C:/Program Files/RStudio/bin/quarto/bin/tools")
# Si la ligne ci-dessus ne fonctionne pas, essayez plutôt celle-ci :
Sys.setenv(RSTUDIO_PANDOC = "C:/Program Files/RStudio/resources/app/bin/quarto/bin/tools")

source("C:/Users/erwan/OneDrive/Documents/Rstudio/Document personnel/Projet/Monthly/Sandoz/Copie/Fonctions.R")

################################################################################
# Configuration du plan de parallélisation
################################################################################
# Utiliser 12 cœurs sur 16 pour laisser de la marge au système
nb_workers <- 12
plan(multisession, workers = nb_workers)

################################################################################
# 2) Définir le calcul du jour d'envoi : le 10 (ou ici 18 pour test) 
#    + gestion du week-end et jours fériés
################################################################################

jours_feries_2025 <- as.Date(c(
  "2025-01-01",
  "2025-04-21",
  "2025-05-01",
  "2025-05-08",
  "2025-05-29",
  "2025-06-09",
  "2025-07-14",
  "2025-08-15",
  "2025-11-01",
  "2025-11-11",
  "2025-12-25"
))

jour_envoi_mensuel <- function(an = year(Sys.Date()), mo = month(Sys.Date())) {
  # ICI, on a mis 18 pour tester, remettez 10 pour votre version finale
  date_theorique <- as.Date(sprintf("%04d-%02d-17", an, mo))
  
  while (weekdays(date_theorique) %in% c("samedi", "dimanche") ||
         date_theorique %in% jours_feries_2025) {
    date_theorique <- date_theorique + 1
  }
  return(date_theorique)
}

jour_envoi <- jour_envoi_mensuel()
message("Le jour d'envoi mensuel calculé est : ", as.character(jour_envoi))

################################################################################
# 3) Vérifier si nous sommes bien ce jour d'envoi
################################################################################

if (Sys.Date() == jour_envoi) {
  message("==> Nous sommes le bon jour d'envoi ! Début du traitement...")
  
  # Définition du répertoire de travail
  setwd("C:/Users/erwan/OneDrive/Documents/Rstudio/Document personnel/Projet/Monthly/Sandoz/Copie/")
  
  options(scipen = 999)  
  
  # (Optionnel) charger d'autres scripts si besoin
  # Par ex. un dossier "Environment" :
  list.files(
    path       = "../../Utils/sandbox/Environment",
    pattern    = "*.R",
    full.names = TRUE
  ) |>
    lapply(source, encoding = "UTF-8") |>
    invisible()
  
  ##############################################################################
  # 3) Récupération de la période & requête SQL pour la liste des pharmacies
  ##############################################################################
  
  # Ici, `mois_precedent()` est déjà chargé depuis connexion_config.R
  date_   <- mois_precedent("sql")
  periode <- date_
  message("Période calculée (SQL) : ", periode)
  
  sql_req <- sprintf("
SELECT n_auto as n_auto_adhpha, cip_crm as cip, mail FROM
(SELECT n_auto_adhpha_artic as n_auto, SUM(qt_vendu_artic * pfht) as sandoz
  FROM os_stat_artic
  INNER JOIN os_labogener_artic ON n_auto_artic_artic = n_auto_artic
  WHERE n_auto_adhpha_artic IN (
    SELECT n_auto_adhpha
    FROM os_completudepha
    WHERE periode_completudepha = %s AND moisok_completudepha = 1
    GROUP BY n_auto_adhpha
    HAVING SUM(moisok_completudepha) = 1
  )
  AND n_auto_adhfour = 401884
  AND periode = %s
  GROUP BY n_auto_adhpha_artic
) t1
INNER JOIN (
  SELECT n_auto_adhpha_artic, SUM(qt_vendu_artic * pfht) AS potentiel
  FROM os_stat_artic
  INNER JOIN os_labogener_artic ON n_auto_artic_artic = n_auto_artic
  WHERE periode = %s
    AND n_auto_adhpha_artic > 2
  GROUP BY n_auto_adhpha_artic
) t2 ON t1.n_auto = t2.n_auto_adhpha_artic AND potentiel <> 0
LEFT JOIN ospharea_crm crm ON crm.serial_ospharm_adherents = n_auto 
LEFT JOIN os_adhpha ON os_adhpha.n_auto_adhpha = crm.serial_ospharm_adherents
LEFT JOIN ospharea_adherents ON finess_adhpha = finess 
WHERE sandoz / potentiel >= 0.4
  AND n_auto IN (
    SELECT n_auto_adhpha
    FROM dbo.os_grp_adhpha
    WHERE n_auto_adhgrp = 406
  )
", periode, periode, periode)
  
  df_pharmacies <- dbGetQuery(con, sql_req)%>%
    filter(n_auto_adhpha %in% c(10501, 11401, 11655, 6852, 9705))
  #view(df_pharmacies) 
  # Pour test, on modifie l'email
  #df_pharmacies$mail <- c("biama@ospharea.com", "adouchin@ospharea.com", "mprudhomme@ospharea.com", "guillaume.de_lorenzi@sandoz.com")  
  message("Pharmacies à traiter : ", nrow(df_pharmacies))
  
  ##############################################################################
  # 4) Fonction de génération et envoi de rapport (MODIFIÉE pour la parallélisation)
  ##############################################################################
  
  genererEtEnvoyerRapport <- function(row_data, working_dir, template_path) {
    # Extraire les données de la ligne
    id_pharma    <- row_data$n_auto_adhpha
    email_pharma <- row_data$mail
    cip_pharma   <- row_data$cip
    
    # Définir le répertoire de travail pour ce processus
    setwd(working_dir)
    
    # Recréer la connexion à la base de données pour ce processus
    # (chaque processus doit avoir sa propre connexion)
    # AJUSTEZ selon votre configuration de connexion
    source("C:/Users/erwan/OneDrive/Documents/Rstudio/Document personnel/Projet/Monthly/Sandoz/Copie/Fonctions.R")
    
    tryCatch({
      # -- 1) Calculs / extraction de données
      maPharma    <- getPharmacieInfo(id_pharma)
      monMois     <- mois_precedent() 
      top_10_data <- top_10_gener(id_pharma, con)
      Lancement   <- calculer_lancements(id_pharma, con)
      top_5_data  <- top_5(id_pharma, con)
      bio <- Biosimilaire(id_pharma, con)
      s_switch    <- Somme_switch(id_pharma, con)
      s_princeps  <- Somme_princeps(id_pharma, con)
      marge       <- s_switch + s_princeps
      
      bio2 <- Biosimilaire_D_Mars(id_pharma, con)
      
      # Estimation du PDM
      gap_total <- tail(bio$`PDM PFHT`, 1)
      tot <- tail(bio$`PFHT autres laboratoires`, 1) + tail(bio$`PFHT Sandoz`, 1)
      gap_30 <- if (gap_total < 30) tot* 0.3 - tail(bio$`PFHT Sandoz`, 1) else 0
      gap_50 <- if (gap_total < 50) tot* 0.5 - tail(bio$`PFHT Sandoz`, 1) else 0
      
      gap_total2 <- tail(bio2$`PDM PFHT`, 1)
      tot2 <- tail(bio2$`PFHT autres laboratoires`, 1) + tail(bio2$`PFHT Sandoz`, 1)
      gap_30_2 <- if (gap_total2 < 30) tot2* 0.3 - tail(bio2$`PFHT Sandoz`, 1) else 0
      gap_50_2 <- if (gap_total2 < 50) tot2* 0.5 - tail(bio2$`PFHT Sandoz`, 1) else 0
      # -- 2) Rendre le RMarkdown en HTML puis convertir en PDF
      # Ajouter un identifiant unique pour éviter les conflits entre processus
      process_id <- Sys.getpid()
      html_file <- paste0("Rapport_", cip_pharma, "_", monMois, "_", process_id, ".html")
      pdf_file  <- paste0("Rapport_", cip_pharma, "_", monMois, "_", process_id, ".pdf")
      
      rmarkdown::render(
        input       = template_path,
        output_file = html_file,
        params = list(
          pharmacie      = maPharma,
          mois_annee     = monMois,
          switch_total   = s_switch,
          princeps_total = s_princeps,
          marge          = marge,
          top_10         = top_10_data,
          top_5          = top_5_data,
          lancement      = Lancement,
          bio            = bio,
          bio2           = bio2,
          gap_total      = gap_total,
          gap_30         = gap_30,
          gap_50         = gap_50,
          gap_total2     = gap_total2,
          gap_30_2       = gap_30_2,
          gap_50_2       = gap_50_2
        )
      )
      
      pagedown::chrome_print(
        input   = html_file,
        output  = pdf_file,
        options = list(
          marginTop           = 0, 
          marginBottom        = 0,
          marginRight         = 0,
          marginLeft          = 0,
          printBackground     = TRUE,
          preferCSSPageSize   = TRUE, 
          displayHeaderFooter = FALSE
        )
      )
      
      ########## Email body #############
      email_body <- paste0('
      <p>Bonjour,</p>
      <p>Nous avons le plaisir de vous faire parvenir votre rapport Générique/Biosimilaire en partenariat avec Sandoz.</p>
      <p>Ce tout nouveau rapport, réfléchi et créé par les équipes de Sandoz et d&#39;Ospharm, va vous permettre d&#39;optimiser votre marge officinale en quelques minutes.</p>
      <p>Votre délégué Sandoz se tient à votre disposition pour le commenter avec vous. Pensez à lui en parler lors de votre prochain contact.</p>
      <p>Cordialement,</p>
      <p>L&#39;équipe Ospharm</p>
  <div style="text-align:left; margin-top: 20px;">
    <img src="https://isaac-1996.github.io/Localisation/logo.png" 
         alt="Logo OSPHARM" 
         style="width:150px; height:auto;">
         
    <p style="margin-top: 10px; color:green;">
      2 Avenue du Gulf Stream<br>
      44380 Pornichet<br>
      <a href="mailto:solutions@ospharea.com" style="color:blue;">solutions@ospharea.com</a><br>
      <a href="tel:+33(0)2 40 53 63 44" style="color:blue;">+33(0)2 40 53 63 44</a><br>
      <a href="https://www.ospharm.com" style="color:blue;">www.ospharm.com</a>
    </p>
  </div>
    ')
      
      # -- 3) Envoi de l'email avec la PJ PDF
      send.mail(
        from         = "Coopérative OSPHARM<solutions@ospharea.com>",
        to           = email_pharma,
        subject      = paste("Votre rapport mensuel du groupement Sandoz", maPharma),
        body         = email_body,
        html         = TRUE,  # indispensable pour que le body soit traité comme HTML
        smtp         = list(
          host.name = Sys.getenv("mailjet_host"),
          port      = Sys.getenv("mailjet_port"),
          user.name = Sys.getenv("mailjet_user"),
          passwd    = Sys.getenv("mailjet_pass"),
          ssl       = TRUE
        ),
        attach.files  = pdf_file,
        authenticate  = TRUE,
        send          = TRUE
      )
      
      # Nettoyer les fichiers temporaires
      if (file.exists(html_file)) file.remove(html_file)
      if (file.exists(pdf_file)) file.remove(pdf_file)
      
      return(list(
        id_pharma = id_pharma,
        email_pharma = email_pharma,
        status = "OK"
      ))
      
    }, error = function(e) {
      return(list(
        id_pharma = id_pharma,
        email_pharma = email_pharma,
        status = paste("ERREUR:", e$message)
      ))
    })
  }
  
  ##############################################################################
  # 5) Traitement parallèle des envois
  ##############################################################################
  
  message("Début du traitement parallèle avec ", nb_workers, " processus...")
  
  # Stocker le répertoire de travail et le chemin du template
  working_dir <- getwd()
  template_path <- "template_0.Rmd"
  
  # Convertir le dataframe en liste de lignes pour future_apply
  pharmacies_list <- split(df_pharmacies, seq(nrow(df_pharmacies)))
  
  # Traitement parallèle
  results_list <- future_lapply(pharmacies_list, function(row_data) {
    genererEtEnvoyerRapport(row_data, working_dir, template_path)
  }, future.seed = TRUE)
  
  # Convertir les résultats en dataframe
  report_results <- do.call(rbind, lapply(results_list, function(x) {
    data.frame(
      id_pharma = x$id_pharma,
      email_pharma = x$email_pharma,
      status = x$status,
      stringsAsFactors = FALSE
    )
  }))
  
  message("Traitement parallèle terminé.")
  
  ##############################################################################
  # 6) Envoyer un bilan à l'administrateur
  ##############################################################################
  
  num_ok  <- sum(report_results$status == "OK")
  num_err <- sum(grepl("ERREUR", report_results$status))
  errors_only <- subset(report_results, grepl("ERREUR", status))
  
  bilan_text <- paste0(
    "Bonjour,\n\n",
    "Bilan de l'envoi des rapports Sandoz du", Sys.Date(),
    " - Envoyés OK : ", num_ok, "\n",
    " - En erreurs : ", num_err, "\n\n",
    "Détails des erreurs :\n"
  )
  
  if (nrow(errors_only) == 0) {
    # S'il n'y a aucune erreur, on indique "Aucune erreur"
    bilan_text <- paste0(bilan_text, "Aucune erreur rencontrée.\n")
  } else {
    # Sinon, on liste les erreurs
    for (j in seq_len(nrow(errors_only))) {
      bilan_text <- paste0(
        bilan_text,
        "ID=", errors_only$id_pharma[j], " | ",
        "Email=", errors_only$email_pharma[j], " | ",
        "Statut=", errors_only$status[j], "\n"
      )
    }
  }
  
  # Envoi du mail de bilan
  send.mail(
    from         = "Coopérative OSPHARM<solutions@ospharea.com>",
    to           = "biama@ospharea.com",  # Admin
    subject      = paste("Bilan d'envoi parallèle -", Sys.Date()),
    body         = bilan_text,
    smtp         = list(
      host.name = Sys.getenv("mailjet_host"),
      port      = Sys.getenv("mailjet_port"),
      user.name = Sys.getenv("mailjet_user"),
      passwd    = Sys.getenv("mailjet_pass"),
      ssl       = TRUE
    ),
    authenticate = TRUE,
    send         = TRUE
  )
  
  message("==> Traitement parallèle terminé, bilan envoyé à l'administrateur.")
  
  # Fermer le plan de parallélisation
  plan(sequential)
  
} else {
  # 7) Si nous ne sommes pas le bon jour
  message("Nous ne sommes PAS le bon jour d'envoi (le 10 ou jour ouvrable suivant). Fin du script.")
}