
%macro Table_mensuelle(indice, anneemois);
	PROC SQL;
		CREATE TABLE rec_mg&indice. AS
			SELECT 
				prs.ben_idt_ano,
				prs.ben_idt_top,
				prs.ben_ama_cod,
				prs.ben_cmu_top,
				prs.grg_aff_cod,
				prs.cai_aff_cod,
				prs.ben_res_dpt, 
				prs.ben_res_com ,
				prs.exe_soi_amd,
				prs.exe_soi_dtd,
				prs.prs_act_qte,
				prs.flx_dis_dtd
			FROM  oravue.ns_prs_f prs
				WHERE 
					prs.exe_soi_amd="&anneemois."
					/*AND substr(prs.ben_dcd_ame,1,4) ne "&annee."*/
					
					/*** A CHANGER SELON L'INDICATEUR SOUHAITE ***/
				AND prs.prs_nat_ref in (1111,1112,1211,1212)
					AND (prs.pse_spe_cod in (1,22,23) OR (prs.pse_spe_cod = 99));
			  		/****/
					
	QUIT;

	DATA Rec_mg&indice.;
		SET Rec_mg&indice.;
		regime=grg_aff_cod;
		annee=substr(exe_soi_amd,1,4);
		mois=substr(exe_soi_amd,5,2);
		top_dpt_res=.;

		/*Création de la variable département de l’organisme : dpt_org*/
		IF substr(cai_aff_cod,1,2)='97' THEN
			dpt_org=substr(cai_aff_cod,1,3);
		ELSE dpt_org=substr(cai_aff_cod,1,2);

		/*Création de la variable code géo : depcom*/
		IF (ben_res_com in ('000','999') OR ben_res_dpt in ('000','099','991','999')) THEN depcom='     ';/*Commune ou département inconnu*/
		ELSE IF ben_res_dpt ne '209' AND substr(ben_res_dpt,1,2) ne '97' THEN depcom=compress(substr(ben_res_dpt,2,2)||ben_res_com);/*Cas général*/
		ELSE IF ben_res_dpt = '209' THEN depcom=compress(substr(ben_res_dpt,1,2)||ben_res_com);/*Corse*/
		ELSE IF (grg_aff_cod = '02A' and substr(ben_res_dpt,1,2)='97') THEN depcom=compress(ben_res_dpt||substr(ben_res_com,2,2));/*DOM pour MSA*/
		ELSE IF ((grg_aff_cod in ('03A','04A','05A')) and substr(ben_res_dpt,1,2)='97') THEN depcom=compress(ben_res_dpt||substr(ben_res_com,2,2));/*DOM pour MSA*/

		/*Création de la variable département ajustée (pour DOM et Corse) remontée pour chaque prestation : dpt_prs*/
		/* Création d'un top top_dpt_res qui indique si la variable département vient 
		du dept de résidence (0) ou de l'organisme d'affiliation le cas échéant (1) */
		IF ben_res_dpt>='001' and ben_res_dpt<='095' THEN
			DO;
				dpt_prs=ben_res_dpt;
				top_dpt_res=0;
			END;
		ELSE IF ben_res_dpt='209' THEN
			DO;
				dpt_prs='020';
				top_dpt_res=0;
			END;
		ELSE IF substr(depcom,1,2)='97' THEN
			DO;
				dpt_prs=substr(depcom,1,3);
				top_dpt_res=0;
			END;

		/* Cas où la commune n'est pas disponible mais le département est disponible */
		ELSE IF substr(ben_res_dpt,1,2)='97' AND depcom not in ('97123','97127') AND depcom<'97700' THEN
			DO;
				dpt_prs=substr(ben_res_dpt,1,3);
				top_dpt_res=0;
			END;/*département de résidence si MSA ou RSI*/

		/* Cas où ni la commune ni le département ne sont disponibles mais le département de l'organisme est retrouvé (tous sauf RSI)*/
		ELSE IF regime in ('01M','01C','02A') AND dpt_org<='95' AND dpt_org ne '975'  THEN
			DO;
				dpt_prs=compress('0'||dpt_org);
				top_dpt_res=1;
			END;/*département de la caisse d'affiliation sinon (hors RSI)*/
		ELSE IF regime in ('01M','01C','02A') and dpt_org<='976' and dpt_org ne '975' THEN
			DO;
				dpt_prs=dpt_org;
				top_dpt_res=1;
			END;/*département de la caisse d'affiliation sinon (hors RSI)*/
		ELSE IF ben_res_dpt='097' and depcom not in ('97123','97127') and depcom<'97700' THEN
			DO;
				dpt_prs='097';
				top_dpt_res=0;
			END;
	run;

%mend;



%macro boucle (annee);
	%do i = 1 %to 12;
		data _null_;
			call symput ('j',put(%eval(&i),z2.));
		run;
		%Table_mensuelle(&i.&annee.,&annee.&j.);
	%end;
	%mend;

	%boucle(2016);
	DATA rec_mg2016;
	SET rec_mg12016 rec_mg22016 rec_mg32016 
		rec_mg42016 rec_mg52016 rec_mg62016
		rec_mg72016 rec_mg82016 rec_mg92016 
		rec_mg102016 rec_mg112016 rec_mg122016;
RUN;


PROC SQL;
	CREATE TABLE sasdata1.rec_mg16 AS 
		SELECT DISTINCT 
			prs.ben_idt_ano, 
			prs.ben_idt_top,
			prs.ben_ama_cod,
			prs.ben_cmu_top,
			prs.dpt_prs,  
			(MIN(prs.mois)) AS mois1, 
			(MAX(prs.mois)) AS mois2, 
			(MAX(input(prs.mois,2.))-MIN(input(prs.mois,2.))) AS duree,
			/* Nb de consult par pseudoNIR et par dept de résidence*/
			(SUM(prs.prs_act_qte)) AS nb_consult, 
			prs.regime, 
			(SUM(prs.top_dpt_res)) AS top_dpt_res2,
			(COUNT (DISTINCT(prs.dpt_prs))) AS nb_dept
		FROM rec_mg2016 prs 
		GROUP BY prs.ben_idt_ano
		ORDER BY prs.ben_idt_ano, prs.top_dpt_res desc,  prs.mois desc; /* conserve deux informations pour les individus dont le département a changé*/
QUIT;

DATA rec_indmg16; 
	SET sasdata1.rec_mg16; 
	BY ben_idt_ano; 
	IF first.ben_idt_ano;
RUN;

DATA orauser.rec_indmg16;
SET rec_indmg16;
RUN;

PROC SQL;
	CREATE TABLE sasdata1.ben_mg_16 AS 
		SELECT 
			prs.ben_idt_ano, 
			prs.ben_idt_top,
			prs.ben_ama_cod,
			prs.ben_cmu_top,
			prs.dpt_prs, 
			prs.nb_dept,
			prs.mois1, 
			prs.mois2, 
			prs.duree,
			prs.nb_consult, 
			prs.regime,
			prs.top_dpt_res2,
			2016- ben.ben_nai_ann as age, 
			ben.ben_sex_cod,      
			ben.ben_dcd_ame
		FROM orauser.rec_indmg16 prs INNER JOIN oravue.ir_iba_r ben 
			ON (prs.ben_idt_ano = ben.ben_idt_ano) 
		ORDER BY ben.ben_idt_ano;
QUIT;
