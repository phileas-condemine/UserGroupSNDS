/*------------------------------------------------------------------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/
/*---------------- MACRO DETERMINANT LA DERNIERE COMMUNE DE RESIDENCE VALIDE SUR UNE PERIODE -----------------*/
/*------------------------------------------------------------------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/


/* Eventuellement sélectionner une période avant de lancer la macro (ex : année glissante avant instauration...) */
/* La macro ne conserve que les régimes : RG hors SLM, MSA et RSI */
/* La macro est adaptée à la présence ou l'absence du rang gémellaire */

/* libent : librairie de la table d'entree */
/* entree : table d'entree qui doit contenir les variables issues de la table PRS suivantes
		=> - l'identifiant patient : ben_nir_psa (+ ben_rng_gem)
		   - ben_res_com, ben_res_dpt  
		   - l'organisme d'affiliation org_aff_ben 
		   - le mois et l'année d'exécution du soin (variables mois et annee) 
Par ex select distinct ben_nir_psa, ben_rng_gem, ben_res_com, ben_res_dpt, org_aff_ben, extract(year from exe_soi_dtd) as annee, extract(month from exe_soi_dtd) as mois */
/* libsor : librairie de la table de sortie */
/* sortie : table de sortie qui contient :
		=> - l'identifiant patient : ben_nir_psa (+ ben_rng_gem)
		   - le code commune corrigé, ou bien le département quand la commune est inconnue (depcom)
		   - une variable indiquant s'il s'agit du département en l'absence du code commune (corr_dpt)
Si la commune et le département sont inconnus, le patient ne figure pas dans la table de sortie 
+ Possibilité de mal classés dans 971, 976 et 97 (COM : Saint Martin, Saint Barthélémy, Nouvelle-Calédonie, Polynésie...)*/
/* andec : année choisie pour le découpage géographique (au 1er janvier), 2011 ou 2012 
		=> choisir 2011 pour joindre aux communes (ou départements) obtenues l'indice de défavorisation 2009 */

%macro depcom_corr(libent=,entree=,libsor=,sortie=,andec=);

/*test l'existence de ben_rng_gem dans la table d'entrée */
%global gemexist;
		proc sql noprint;
        SELECT count(*)
        INTO :varexist
        FROM DICTIONARY.COLUMNS
        WHERE libname="%UPCASE(&libent)" AND memname="%UPCASE(&entree)"
                   AND (name="ben_rng_gem" or name="BEN_RNG_GEM");
    quit;
%IF &varexist>0 %then %let gemexist=1;%ELSE %let gemexist=0;
%put &varexist &gemexist;

data entree;
set &libent..&entree.;
if substr(org_aff_ben,4,2) in ('97','20') then dpt_org=substr(org_aff_ben,4,3);else dpt_org=substr(org_aff_ben,4,2);
regime=substr(org_aff_ben,1,3);
where substr(org_aff_ben,1,3) in ('01C','02A','03A');
run;
proc sql;
create table entree as select 
distinct ben_nir_psa, %if &gemexist=1 %then ben_rng_gem,; ben_res_dpt, ben_res_com, dpt_org, regime, annee, mois 
from entree;
quit;

proc sql;
create table inter as 
select ben_res_dpt, ben_res_com, regime, %if &gemexist=1 %then count(distinct ben_nir_psa||put(ben_rng_gem,1.)); %else count(distinct ben_nir_psa); as nb_conso 
from entree 
group by ben_res_dpt, ben_res_com, regime;
quit;

data inter;
set inter;
if (ben_res_com in ('000','999') or ben_res_dpt in ('000','099','991','999')) then delete;/*Commune ou département inconnu*/
else if ben_res_dpt ne '209' and substr(ben_res_dpt,1,2) ne '97' then depcom=compress(substr(ben_res_dpt,2,2)||ben_res_com);/*Cas général*/
else if ben_res_dpt = '209' then depcom=compress(substr(ben_res_dpt,1,2)||ben_res_com);/*Corse*/
else if (regime='MSA' and substr(ben_res_dpt,1,2)='97') then depcom=compress(substr(ben_res_dpt,1,2)||ben_res_com);/*DOM pour MSA*/
else if (regime='RSI' and substr(ben_res_dpt,1,2)='97') then depcom=compress(ben_res_dpt)||substr(ben_res_com,2,2);/*DOM pour RSI*/
run;

/*Utilisation de la table de correction des codes communes en fonction de l'année choisie*/
proc sort data=RFCOMMUN.corrections_com&andec._new out=corrections_com&andec._new;by depcom;run;
proc sort data=inter;by depcom;run;
data inter;
merge inter(in=a) corrections_com&andec._new;
by depcom;
if a;
run;

data inter;
set inter;
if depcom_corr ne '' then depcom=depcom_corr;
drop depcom_corr;
run;

data appartenance_geo_&andec.;
set RFCOMMUN.appartenance_geo_&andec.;
if substr(codgeo,1,2)='2A' then depcom=compress('20'||substr(codgeo,3,3));
else if substr(codgeo,1,2)='2B' then depcom=compress('20'||substr(codgeo,3,3));
else depcom=codgeo;
run;

proc sort data=appartenance_geo_&andec.;by depcom;run;
proc sort data=inter;by depcom;run;
data inter;
merge inter(in=a) appartenance_geo_&andec.(in=b keep=depcom);
by depcom;
if a;
if b then top_insee=1;else top_insee=0;
run;

proc sort data=entree out=inter2;by ben_res_dpt ben_res_com regime;run;
proc sort data=inter;by ben_res_dpt ben_res_com regime;run;
data inter2;
merge inter2(in=a) inter;
by ben_res_dpt ben_res_com regime;
if a;
run;

proc sort data=inter2;by ben_nir_psa %if &gemexist=1 %then ben_rng_gem; descending annee descending mois;run;
data com;
set inter2;
if (top_insee=0 or top_insee=.) and depcom not in ('97701','97801') then delete;
run;
data com;
set com;
by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;
if %if &gemexist=1 %then first.ben_rng_gem; %else first.ben_nir_psa;;
keep ben_nir_psa %if &gemexist=1 %then ben_rng_gem; depcom;
run;

data dpt;
set inter2;
if ben_res_dpt in ('000','099','999') then delete;
run;
data dpt;
set dpt;
by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;
if %if &gemexist=1 %then first.ben_rng_gem; %else first.ben_nir_psa;;
rename ben_res_dpt=dpt
	   depcom=depcom2;
keep ben_nir_psa %if &gemexist=1 %then ben_rng_gem; ben_res_dpt depcom;
run;

data dpt_org;
set inter2;
if dpt_org='99' then delete;
run;
data dpt_org;
set dpt_org;
by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;
if %if &gemexist=1 %then first.ben_rng_gem; %else first.ben_nir_psa;;
rename depcom=depcom3;
keep ben_nir_psa %if &gemexist=1 %then ben_rng_gem; dpt_org regime depcom;
run;

proc sort data=com;by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;run;
proc sort data=dpt;by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;run;
proc sort data=dpt_org;by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;run;
data inter3;
merge com dpt dpt_org;
by ben_nir_psa %if &gemexist=1 %then ben_rng_gem;;
run;

data inter3;
set inter3;
format dpt_f $5.;
if depcom='' then do;
	if (dpt>='001' and dpt<='095') then dpt_f=substr(dpt,2,2);/*département de résidence si France métro (hors Corse)*/
	else if regime in ('01C','02A') and dpt_org<='976' and dpt_org ne '975' and depcom3 not in ('97123','97127') and depcom3<'97700' then dpt_f=dpt_org;/*département de la caisse d'affiliation sinon (hors RSI)*/
	else if substr(dpt,1,2)='97' and depcom2 not in ('97123','97127') and depcom2<'97700' then dpt_f=substr(dpt,1,3);/*département de résidence si MSA ou RSI*/
	else if dpt='097' and depcom2 not in ('97123','97127') and depcom2<'97700' then dpt_f=substr(dpt,2,2);
	else if dpt='209' then dpt_f=substr(dpt,1,2);
				  end;
run;

data &libsor..&sortie.;
set inter3;
if depcom='' and dpt_f>='01' and dpt_f ne '975' and dpt_f<='976' then do;depcom=dpt_f;corr_dpt=1;end;
else if depcom='' then delete;
else corr_dpt=0; 
drop dpt_f regime dpt_org dpt depcom2 depcom3;
run;

%mend;

/*%depcom_corr(libent=,entree=,libsor=,sortie=,andec=);*/


/*------------------------------------------------------------------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/
/*------------------------ MACRO ASSOCIANT L'INDICE DE DEFAVORISATION A LA COMMUNE ---------------------------*/
/*------------------------------------------------------------------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

/* libent : librairie de la table d'entree */
/* entree : table d'entree (= table de sortie de la macro depcom_corr) */
/* libsor : librairie de la table de sortie */
/* sortie : table de sortie qui contient :
		=> - l'identifiant patient : ben_nir_psa (+ ben_rng_gem)
		   - le code commune corrigé, ou bien le département quand la commune est inconnue (depcom)
		   - la variable corr_dpt
		   - l'indice de défavorisation (FDEPXX)
		   - le quintile de l'indice de défavorisation (quintile) (pondéré par le nombre d'habitants de la commune)
		   - une variable indiquant si depcom appartient à un DOM (dom) (pas d'indice dans ce cas) */
/* andefavo : année AAAA de l'indice de défavorisation 
		=> choisir 2009 si choix de 2011 pour la macro depcom_corr */

%macro com_defavo(libent=,entree=,libsor=,sortie=,andefavo=);

data defa;
set consopat.defa_uu&andefavo.;
if substr(depcom,1,2)='2A' then depcom=compress('20'||substr(depcom,3,3));
if substr(depcom,1,2)='2B' then depcom=compress('20'||substr(depcom,3,3));
rename quintile_pop=quintile;
where substr(depcom,1,2)<='95';
keep depcom fdep%substr(&andefavo.,3,2) quintile_pop;
run;
data defa_dpt;
set RFCOMMUN.defa_dpt&andefavo.;
if dpt='2A' then dpt='201';
if dpt='2B' then dpt='202';
rename dpt=depcom
	   FDEP%substr(&andefavo.,3,2)_dpt=FDEP%substr(&andefavo.,3,2);
run;

data defa;
set defa defa_dpt;
run;

proc sort data=&libent..&entree. out=entree;by depcom;run;
proc sort data=defa;by depcom;run;
data entree;
merge entree(in=a) defa;
by depcom;
if a;
run;

data &libsor..&sortie.;
set entree;
if substr(depcom,1,2)='97' then dom=1;else dom=0;
run;

%mend;

/*%com_defavo(libent=,entree=,libsor=,sortie=,andefavo=);*/
