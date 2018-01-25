/*** 1. Préparation de la table ***/

%macro Table_mensuelle(indice, ddmmaaaa_flux, annee);
	PROC SQL;
		CREATE TABLE rec_mg&indice. AS
			SELECT 
				prs.ben_nir_psa,
				prs.ben_rng_gem, 
				(prs.ben_nir_psa||put(prs.ben_rng_gem,1.)) as ben_nir_uni,
				prs.ben_res_dpt, 
				prs.ben_res_com ,
				prs.org_aff_ben,
				prs.exe_soi_amd,
				prs.prs_act_qte
			FROM  oravue.er_prs_f prs
				WHERE 
					substr(prs.exe_soi_amd,1,4)="&annee."
					AND (prs.cpl_maj_top<2) 
					AND substr(prs.ben_dcd_ame,1,4) ne "&annee."
					AND  prs.flx_dis_dtd = dhms(mdy(input(substr(put(&ddmmaaaa_flux.,8.),3,2),2.)/*mois*/,input(substr(put(&ddmmaaaa_flux.,8.),1,2),2.)/*jour*/,
					input(substr(put(&ddmmaaaa_flux.,8.),5,4),4.))/*annee*/,0,0,0)
					AND prs.dpn_qlf ne 71 
					AND prs.prs_dpn_qlp ne 71

					/*** A CHANGER SELON L'INDICATEUR SOUHAITE ***/
					AND prs.prs_nat_ref in (1111,1112,1211,1212)
					AND (prs.pse_spe_cod in (1,22,23) OR (prs.pse_spe_cod = 99));
					/****/
	QUIT;

	/* Etape pour recoder le département dans chaque observation */
	DATA Rec_MG&indice.;
		SET Rec_MG&indice.;
		regime=substr(org_aff_ben,1,3);
		annee=substr(exe_soi_amd,1,4);
		mois=substr(exe_soi_amd,5,2);
		top_dpt_res=.;

		/*Création de la variable département de l’organisme : dpt_org*/
		IF substr(org_aff_ben,4,2)='97' THEN
			dpt_org=substr(org_aff_ben,4,3);
		ELSE dpt_org=substr(org_aff_ben,4,2);

		/*Création de la variable code géo : depcom*/
		IF (ben_res_com in ('000','999') OR ben_res_dpt in ('000','099','991','999')) THEN depcom='     ';/*Commune ou département inconnu*/
		ELSE IF ben_res_dpt ne '209' AND substr(ben_res_dpt,1,2) ne '97' THEN depcom=compress(substr(ben_res_dpt,2,2)||ben_res_com);/*Cas général*/
		ELSE IF ben_res_dpt = '209' THEN depcom=compress(substr(ben_res_dpt,1,2)||ben_res_com);/*Corse*/
		ELSE IF (regime = '02A' and substr(ben_res_dpt,1,2)='97') THEN depcom=compress(ben_res_dpt||substr(ben_res_com,2,2));/*DOM pour MSA*/
		ELSE IF ((regime in ('03A','04A','05A')) and substr(ben_res_dpt,1,2)='97') THEN depcom=compress(ben_res_dpt||substr(ben_res_com,2,2));/*DOM pour MSA*/

		/*Création de la variable département ajustée (pour DOM et Corse) remontée pour chaque prestation : dpt_prs*/
		/* Création d'un top top_dpt_res qui indique si la variable département vient 
		du dept de résidence (1) ou de l'organisme d'affiliation le cas échéant (0) */
		IF ben_res_dpt>='001' and ben_res_dpt<='095' THEN
			DO;
				dpt_prs=ben_res_dpt;
				top_dpt_res=1;
			END;
		ELSE IF ben_res_dpt='209' THEN
			DO;
				dpt_prs='020';
				top_dpt_res=1;
			END;
		ELSE IF substr(depcom,1,2)='97' THEN
			DO;
				dpt_prs=substr(depcom,1,3);
				top_dpt_res=1;
			END;

		/* Cas où la commune n'est pas disponible mais le département est disponible */
		ELSE IF substr(ben_res_dpt,1,2)='97' AND depcom not in ('97123','97127') AND depcom<'97700' THEN
			DO;
				dpt_prs=substr(ben_res_dpt,1,3);
				top_dpt_res=1;
			END;/*département de résidence si MSA ou RSI*/

		/* Cas où ni la commune ni le département ne sont disponibles mais le département de l'organisme est retrouvé (tous sauf RSI)*/
		ELSE IF regime in ('01M','01C','02A') AND dpt_org<='95' AND dpt_org ne '975'  THEN
			DO;
				dpt_prs=compress('0'||dpt_org);
				top_dpt_res=0;
			END;/*département de la caisse d'affiliation sinon (hors RSI)*/
		ELSE IF regime in ('01M','01C','02A') and dpt_org<='976' and dpt_org ne '975' THEN
			DO;
				dpt_prs=dpt_org;
				top_dpt_res=0;
			END;/*département de la caisse d'affiliation sinon (hors RSI)*/
		ELSE IF ben_res_dpt='097' and depcom not in ('97123','97127') and depcom<'97700' THEN
			DO;
				dpt_prs='097';
				top_dpt_res=1;
			END;
	run;

%mend;



/* Macro pour faire tourner la requête sur 18 mois de remontées de données */
%macro boucle (annee);
	%do i = 2 %to 12;
		data _null_;
			call symput ('j',put(%eval(&i),z2.));
		run;
		%Table_mensuelle(&annee.&i.,01&j.&annee.,&annee.);
	%end;

	%let anb=%eval(&annee+1);

	%do i = 1 %to 6;
		data _null_;
			call symput ('j',put(%eval(&i),z2.));
		run;
		%Table_mensuelle(&anb.&i.,01&j.&anb.,&annee.);
	%end;
%mend;
%boucle(2016);

DATA rec_mg2016;
	SET rec_mg20162 rec_mg20163 rec_mg20164 
		rec_mg20165 rec_mg20166 rec_mg20167 
		rec_mg20168 rec_mg20169 rec_mg201610 
		rec_mg201611 rec_mg201612 rec_mg20171 
		rec_mg20172 rec_mg20173 rec_mg20174
		rec_mg20175 rec_mg20176;
RUN;


/* Agrégation des informations par couple NIR pseudonymisé+DEPT de résidence+Regime : mois minimum,
mois maximum, durée entre les soins et nombre de consultations*/
PROC SQL;
	CREATE TABLE sasdata1.rec_mg16 AS 
		SELECT DISTINCT 
			prs.ben_nir_psa, 
			prs.ben_rng_gem, 
			prs.ben_nir_uni, 
			prs.dpt_prs,  
			(MIN(prs.mois)) AS mois1, 
			(MAX(prs.mois)) AS mois2, 
			(MAX(input(prs.mois,2.))-MIN(input(prs.mois,2.))) AS duree, 
			/* Nb de consult par pseudoNIR (tous depts de résidence)*/
			(SUM(prs.prs_act_qte)) AS Nb_consult, 
			(SUM(prs.top_dpt_res)) AS top_dpt_res2, 
			(COUNT (DISTINCT(prs.dpt_prs))) AS nb_dept, 
			prs.regime, 
			prs.top_dpt_res 
		FROM rec_mg2016 prs 
		GROUP BY prs.ben_nir_psa, prs.ben_rng_gem 
		ORDER BY prs.ben_nir_psa, prs.ben_rng_gem, prs.top_dpt_res desc,  prs.mois desc; /* conserve deux informations pour les individus dont le département a changé*/
QUIT;



/*Création de la base annuelle avec une seule ligne par individu (NIR pseudo) : 
  D'après le tri réalisé à l'avant-dernière étape, on privilégie le département non codé 
à partir de l'organisme (top_dpt_res descending) puis celui du dernier mois de soin (mois2 descending) */

DATA rec_indmg16; 
	SET sasdata1.rec_mg16; 
	BY ben_nir_uni; 
	IF first.ben_nir_uni;
RUN;


 /* Jointure avec le référentiel de béneficiaires pour repérer les individus doublons 
et récuperer les informations individuelles administratives */
DATA orauser.rec_indmg16;
	SET rec_indmg16;
RUN;

PROC SQL;
	CREATE TABLE sasdata1.ben_mg_16 AS 
		SELECT 
			prs.ben_nir_psa, 
			prs.ben_nir_uni, 
			prs.ben_rng_gem, 
			prs.dpt_prs, 
			prs.nb_dept,
			prs.mois1, 
			prs.mois2, 
			prs.duree,
			prs.nb_consult, 
			prs.regime,
			prs.top_dpt_res2,
			ben.ben_nai_ann, 
			ben.ben_sex_cod,      
			ben.ben_dcd_ame,
			ben.ben_res_dpt,
			ben.ben_res_com,
			ben.ben_idt_ano,
			ben.ben_idt_top,
			ben.ben_cdi_nir
		FROM orauser.rec_indmg16 prs INNER JOIN oravue.ir_ben_r ben 
			ON (prs.ben_nir_psa = ben.ben_nir_psa) AND (prs.ben_rng_gem  = ben.ben_rng_gem)
		ORDER BY ben.ben_idt_ano;
QUIT;

/* Création de la variable du département (Corse et DOM) à partir de BEN_RES_DPT du référentiel de bénéficiaire*/
DATA sasdata1.ben_mg_16;
	SET sasdata1.ben_mg_16;
	IF (ben_res_dpt>='001' and ben_res_dpt<='095') or (substr(ben_res_dpt,1,2)='97') 
		THEN dpt_ben=ben_res_dpt;/*Cas général*/
	ELSE IF ben_res_dpt = '209'  THEN dpt_ben='020';/*Corse*/
	ELSE IF ben_res_dpt='097' and ben_res_com not in ('000','999') 
	THEN dpt_ben=compress(substr(ben_res_dpt,2,2)||substr(ben_res_com,1,1)); /*DOM */
	ELSE dpt_ben="";/*Commune ou département inconnu*/
RUN;

/* Agrégation des informations par NIR unique (BEN_IDT_ANO)*/
/* Classement permettant de privilégier les observations où le département coïncide entre le référentiel et la table
prestations, puis le département associé au dernier mois de soin */
PROC SQL;
	CREATE TABLE ben_mg_16 AS
		SELECT DISTINCT *,
	/* Indicatrice de différence entre département du référentiel et département de la table prestations */
		CASE
			WHEN (dpt_ben = dpt_prs) THEN 1 
			WHEN (dpt_ben ne dpt_prs) THEN 0 
			END
		AS dpt_diff,
		/* Variables mois et durée recalculées pour individus doublons */
		(MIN(t.mois1)) AS moismin, 
		(MAX(t.mois2)) AS moismax, 
		(MAX(input(t.mois2,2.))-MIN(input(t.mois1,2.))) AS duree2, /* Somme des consultations pour individus doublons */
		(SUM(t.nb_consult)) AS Nb_consult, 
		(SUM(t.top_dpt_res2)) AS top_dpt_res, /*Comptage du nombre de départements différents pour un même individu par ajout à la variable déjà créée */
		((COUNT (DISTINCT(t.dpt_prs)))+t.nb_dept-1) AS nb_dept2 
	FROM sasdata1.ben_mg_16 t 
	GROUP BY ben_idt_ano 
	ORDER BY ben_idt_ano, dpt_diff desc, mois2 DESC, duree DESC;
QUIT;

/*Création de la base annuelle  avec une seule ligne par individu (BEN_NIR_ANO) : 
 D'après le tri réalisé à l'avant-dernière étape, on privilégie le département identique dans les deux bases (dpt_diff=0)
puis celui du dernier mois de soin (mois2 descending) puis celui associé à la plus longue durée entre les soins (duree descending)*/
DATA ben_mg16uniq; 
	SET ben_mg_16; 
	BY ben_idt_ano; 
	IF first.ben_idt_ano; 
RUN;

/* Constitution de la variable finale du département : en priorité la variable du référentiel,
sinon la variable de la table prestations */

DATA sasdata1.ben_mg16uniq;
	SET ben_mg16uniq;
	IF dpt_diff=1 THEN
		dpt=dpt_ben;
	ELSE IF dpt_prs<='095' and dpt_prs>='001' THEN
		dpt=dpt_prs;
	ELSE IF substr(dpt_prs,1,2)='97' and dpt_prs ne '975' THEN
		dpt=dpt_prs;
	ELSE IF dpt_ben<='095' and dpt_ben>='001' THEN
		dpt=dpt_ben;
	ELSE IF substr(dpt_ben,1,2)='97' and dpt_ben ne '975' THEN
		dpt=dpt_ben;
	ELSE dpt='';
	age=2017-put(ben_nai_ann,4.);
	/*Indicatrice du département codé sur l'organisme*/
	IF top_dpt_res=0 THEN
		top_dpt_org=1;
	ELSE top_dpt_org=0;
RUN;

DATA base_finale;
	SET sasdata1.ben_mg16uniq;
	KEEP dpt dpt_prs dpt_ben  dpt_diff top_dpt_org 
	ben_cdi_nir regime ben_dcd_ame ben_idt_top ben_sex_cod 
	duree2 moismin moismax nb_consult nb_dept2;
	/*WHERE dpt not in ("970","975","979");*/
RUN;
