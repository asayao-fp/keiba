#ifndef __JV_DATA_STRUCT
#define __JV_DATA_STRUCT


//========================================================================
//  JRA-VAN Data Lab. JV-Data構造体
//
//
//   作成: JRA-VAN ソフトウェア工房
//
//========================================================================
//   (C) Copyright JRA SYSTEM SERVICE CO.,LTD. 2007 All rights reserved
//========================================================================


//////////////////// 共通構造体 ////////////////////


//<年月日>
typedef struct
{
		char		Year[4];                   //年
		char		Month[2];                  //月
		char		Day[2];                    //日
}				_YMD;


//<時分秒>
typedef struct
{
		char		Hour[2];                   //時
		char		Minute[2];                 //分
		char		Second[2];                 //秒
}				_HMS;


 //<時分>
typedef struct
{
		char		Hour[2];                   //時
		char		Minute[2];                 //分
}				_HM;


//<月日時分>
typedef struct
{
		char		Month[2];                  //月
		char		Day[2];                    //日
		char		Hour[2];                   //時
		char		Minute[2];                 //分
}				_MDHM;


//<レコードヘッダ>
typedef struct
{
		char		RecordSpec[2];             //レコード種別
		char		DataKubun[1];              //データ区分
		_YMD		MakeDate;                  //データ作成年月日
}				_RECORD_ID;


//<競走識別情報１>
typedef struct
{
		char		Year[4];                   //開催年
		char		MonthDay[4];               //開催月日
		char		JyoCD[2];                  //競馬場コード
		char		Kaiji[2];                  //開催回[第N回]
		char		Nichiji[2];                //開催日目[N日目]
		char		RaceNum[2];                //レース番号
}				_RACE_ID;


//<競走識別情報２>
typedef struct
{
		char		Year[4];                   //開催年
		char		MonthDay[4];               //開催月日
		char		JyoCD[2];                  //競馬場コード
		char		Kaiji[2];                  //開催回[第N回]
		char		Nichiji[2];                //開催日目[N日目]
}				_RACE_ID2;


//<着回数（サイズ3byte）>
typedef struct
{
		char		Chakukaisu[6][3];
}				_CHAKUKAISU3_INFO;

//<着回数（サイズ4byte）>
typedef struct
{
		char		Chakukaisu[6][4];
}				_CHAKUKAISU4_INFO;

//<着回数（サイズ5byte）>
typedef struct
{
		char		Chakukaisu[6][5];
}				_CHAKUKAISU5_INFO;

//<着回数（サイズ6byte）>
typedef struct
{
		char		Chakukaisu[6][6];
}				_CHAKUKAISU6_INFO;


//<本年・累計成績情報>
typedef struct
{
		char		SetYear[4];                //設定年
		char		HonSyokinTotal[10];        //本賞金合計
		char		FukaSyokin[10];            //付加賞金合計
		char		ChakuKaisu[6][6];          //着回数
}				_SEI_RUIKEI_INFO;


//<最近重賞勝利情報>
typedef struct
{
		_RACE_ID	SaikinJyusyoid;             //<年月日場回日R>
		char		Hondai[60];                 //競走名本題
		char		Ryakusyo10[20];             //競走名略称10字
		char		Ryakusyo6[12];              //競走名略称6字
		char		Ryakusyo3[6];               //競走名略称3字
		char		GradeCD[1];                 //グレードコード
		char		SyussoTosu[2];              //出走頭数
		char		KettoNum[10];               //血統登録番号
		char		Bamei[36];                  //馬名
}				_SAIKIN_JYUSYO_INFO;



//<本年・前年・累計成績情報>
typedef struct
{
		char		SetYear[4];                 //設定年
		char		HonSyokinHeichi[10];        //平地本賞金合計
		char		HonSyokinSyogai[10];        //障害本賞金合計
		char		FukaSyokinHeichi[10];       //平地付加賞金合計
		char		FukaSyokinSyogai[10];       //障害付加賞金合計
		_CHAKUKAISU6_INFO		ChakuKaisuHeichi;     //平地着回数
		_CHAKUKAISU6_INFO		ChakuKaisuSyogai;     //障害着回数
		_CHAKUKAISU6_INFO		ChakuKaisuJyo[20];    //競馬場別着回数
		_CHAKUKAISU6_INFO		ChakuKaisuKyori[6];   //距離別着回数
}				_HON_ZEN_RUIKEISEI_INFO;


//<レース情報>
typedef struct
{
		char		YoubiCD[1];                //曜日コード
		char		TokuNum[4];                //特別競走番号
		char		Hondai[60];                //競走名本題
		char		Fukudai[60];               //競走名副題
		char		Kakko[60];                 //競走名カッコ内
		char		HondaiEng[120];            //競走名本題欧字
		char		FukudaiEng[120];           //競走名副題欧字
		char		KakkoEng[120];             //競走名カッコ内欧字
		char		Ryakusyo10[20];            //競走名略称１０字
		char		Ryakusyo6[12];             //競走名略称６字
		char		Ryakusyo3[6];              //競走名略称３字
		char		Kubun[1];                  //競走名区分
		char		Nkai[3];                   //重賞回次[第N回]
}				_RACE_INFO;

//<天候・馬場状態>
typedef struct
{
		char		TenkoCD[1];                //天候コード
		char		SibaBabaCD[1];             //芝馬場状態コード
		char		DirtBabaCD[1];             //ダート馬場状態コード
}				_TENKO_BABA_INFO;


//<競走条件コード>
typedef struct
{
		char		SyubetuCD[2];              //競走種別コード
		char		KigoCD[3];                 //競走記号コード
		char		JyuryoCD[1];               //重量種別コード
		char		JyokenCD[5][3];            //競走条件コード
}				_RACE_JYOKEN;


//<騎手変更情報>
typedef struct
{
		char		Futan[3];                 //負担重量
		char		KisyuCode[5];             //騎手コード
		char		KisyuName[34];            //騎手名
		char		MinaraiCD[1];             //騎手見習コード
}				_JC_INFO;


//<発走時刻変更情報>
typedef struct
{
		char		Ji[2];                 	 //時
		char		Fun[2];             	 //分
}				_TC_INFO;


//<コース変更情報>
typedef struct
{
		char		Kyori[4];                //距離
		char		TruckCD[2];              //トラックコード
}				_CC_INFO;


//////////////////// データ構造体 ////////////////////


//**** １．特別登録馬 ****************************************
typedef struct{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_RACE_INFO	RaceInfo;                 //<レース情報>
		char		GradeCD[1];               //グレードコード
		_RACE_JYOKEN	JyokenInfo;               //<競走条件コード>
		char		Kyori[4];                 //距離
		char		TrackCD[2];               //トラックコード
		char		CourseKubunCD[2];         //コース区分
		_YMD		HandiDate;                //ハンデ発表日
		char		TorokuTosu[3];            //登録頭数

		struct _TOKUUMA_INFO                      //<登録馬毎情報>
                {
				char		Num[3];                    //連番
				char		KettoNum[10];              //血統登録番号
				char		Bamei[36];                 //馬名
				char		UmaKigoCD[2];              //馬記号コード
				char		SexCD[1];                  //性別コード
				char		TozaiCD[1];                //調教師東西所属コード
				char		ChokyosiCode[5];           //調教師コード
				char		ChokyosiRyakusyo[8];       //調教師名略称
				char		Futan[3];                  //負担重量
				char		Koryu[1];                  //交流区分
		}				TokuUmaInfo[300];

		char		crlf[2];                  //レコード区切
}				JV_TK_TOKUUMA;


//****** ２．レース詳細 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_RACE_INFO	RaceInfo;                 //<レース情報>
		char		GradeCD[1];               //グレードコード
		char		GradeCDBefore[1];         //変更前グレードコード
		_RACE_JYOKEN	JyokenInfo;               //<競走条件コード>
		char		JyokenName[60];           //競走条件名称
		char		Kyori[4];                 //距離
		char		KyoriBefore[4];           //変更前距離
		char		TrackCD[2];               //トラックコード
		char		TrackCDBefore[2];         //変更前トラックコード
		char		CourseKubunCD[2];         //コース区分
		char		CourseKubunCDBefore[2];   //変更前コース区分
		char		Honsyokin[7][8];          //本賞金
		char		HonsyokinBefore[5][8];    //変更前本賞金
		char		Fukasyokin[5][8];         //付加賞金
		char		FukasyokinBefore[3][8];   //変更前付加賞金
		char		HassoTime[4];             //発走時刻
		char		HassoTimeBefore[4];       //変更前発走時刻
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		NyusenTosu[2];            //入線頭数
		_TENKO_BABA_INFO        TenkoBaba;        //天候・馬場状態コード
		char		LapTime[25][3];           //ラップタイム
		char		SyogaiMileTime[4];        //障害マイルタイム
		char		HaronTimeS3[3];           //前３ハロンタイム
		char		HaronTimeS4[3];           //前４ハロンタイム
		char		HaronTimeL3[3];           //後３ハロンタイム
		char		HaronTimeL4[3];           //後４ハロンタイム

		struct _CORNER_INFO                       //<コーナー通過順位>
                {
				char		Corner[1];                //コーナー
				char		Syukaisu[1];              //周回数
				char		Jyuni[70];                 //各通過順位
		}				CornerInfo[4];

		char		RecordUpKubun[1];         //レコード更新区分
		char		crlf[2];                  //レコード区切り
}				JV_RA_RACE;


//****** ３．馬毎レース情報 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		char		Wakuban[1];               //枠番
		char		Umaban[2];                //馬番
		char		KettoNum[10];             //血統登録番号
		char		Bamei[36];                //馬名
		char		UmaKigoCD[2];             //馬記号コード
		char		SexCD[1];                 //性別コード
		char		HinsyuCD[1];              //品種コード
		char		KeiroCD[2];               //毛色コード
		char		Barei[2];                 //馬齢
		char		TozaiCD[1];               //東西所属コード
		char		ChokyosiCode[5];          //調教師コード
		char		ChokyosiRyakusyo[8];      //調教師名略称
		char		BanusiCode[6];            //馬主コード
		char		BanusiName[64];           //馬主名
		char		Fukusyoku[60];            //服色標示
		char		reserved1[60];            //予備
		char		Futan[3];                 //負担重量
		char		FutanBefore[3];           //変更前負担重量
		char		Blinker[1];               //ブリンカー使用区分
		char		reserved2[1];             //予備
		char		KisyuCode[5];             //騎手コード
		char		KisyuCodeBefore[5];       //変更前騎手コード
		char		KisyuRyakusyo[8];         //騎手名略称
		char		KisyuRyakusyoBefore[8];   //変更前騎手名略称
		char		MinaraiCD[1];             //騎手見習コード
		char		MinaraiCDBefore[1];       //変更前騎手見習コード
		char		BaTaijyu[3];              //馬体重
		char		ZogenFugo[1];             //増減符号
		char		ZogenSa[3];               //増減差
		char		IJyoCD[1];                //異常区分コード
		char		NyusenJyuni[2];           //入線順位
		char		KakuteiJyuni[2];          //確定着順
		char		DochakuKubun[1];          //同着区分
		char		DochakuTosu[1];           //同着頭数
		char		Time[4];                  //走破タイム
		char		ChakusaCD[3];             //着差コード
		char		ChakusaCDP[3];            //+着差コード
		char		ChakusaCDPP[3];           //++着差コード
		char		Jyuni1c[2];               //1コーナーでの順位
		char		Jyuni2c[2];               //2コーナーでの順位
		char		Jyuni3c[2];               //3コーナーでの順位
		char		Jyuni4c[2];               //4コーナーでの順位
		char		Odds[4];                  //単勝オッズ
		char		Ninki[2];                 //単勝人気順
		char		Honsyokin[8];             //獲得本賞金
		char		Fukasyokin[8];            //獲得付加賞金
		char		reserved3[3];             //予備
		char		reserved4[3];             //予備
		char		HaronTimeL4[3];           //後４ハロンタイム
		char		HaronTimeL3[3];           //後３ハロンタイム

		struct _CHAKUUMA_INFO                     //<1着馬[相手馬]情報>
                {
				char		KettoNum[10];     //血統登録番号
				char		Bamei[36];        //馬名
		}			ChakuUmaInfo[3];

		char		TimeDiff[4];              //タイム差
		char		RecordUpKubun[1];         //レコード更新区分
		char		DMKubun[1];               //マイニング区分
		char		DMTime[5];                //マイニング予想走破タイム
		char		DMGosaP[4];               //予測誤差[信頼度]＋
		char		DMGosaM[4];               //予測誤差[信頼度]－
		char		DMJyuni[2];               //マイニング予想順位
		char		KyakusituKubun[1];        //今回レース脚質判定
		char		crlf[2];                  //レコード区切り
}				JV_SE_RACE_UMA;


//****** ４．払戻 ****************************************
//<払戻情報１ 単・複・枠>
typedef struct
{
		char		Umaban[2];                //馬番
		char		Pay[9];                   //払戻金
		char		Ninki[2];                 //人気順 
}			_PAY_INFO1;


//<払戻情報２ 馬連・ワイド・予備・馬単>
typedef struct
{
		char		Kumi[4];                  //組番
		char		Pay[9];                   //払戻金
		char		Ninki[3];                 //人気順 
}			_PAY_INFO2;


//<払戻情報３ ３連複>
typedef struct{ 
		char		Kumi[6];                  //組番
		char		Pay[9];                   //払戻金
		char		Ninki[3];                 //人気順 
}			_PAY_INFO3;


//<払戻情報４ ３連単>
typedef struct
{
		char		Kumi[6];                  //組番
		char		Pay[9];                   //払戻金
		char		Ninki[4];                 //人気順
}			_PAY_INFO4;


typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		FuseirituFlag[9][1];      //不成立フラグ
		char		TokubaraiFlag[9][1];      //特払フラグ
		char		HenkanFlag[9][1];         //返還フラグ
		char		HenkanUma[28][1];         //返還馬番情報[馬番01～28]
		char		HenkanWaku[8][1];         //返還枠番情報[枠番1～8]
		char		HenkanDoWaku[8][1];       //返還同枠情報[枠番1～8]
		_PAY_INFO1		PayTansyo[3];         //<単勝払戻>
		_PAY_INFO1		PayFukusyo[5];        //<複勝払戻>
		_PAY_INFO1		PayWakuren[3];        //<枠連払戻>
		_PAY_INFO2		PayUmaren[3];         //<馬連払戻>
		_PAY_INFO2		PayWide[7];           //<ワイド払戻>
		_PAY_INFO2		PayReserved1[3];      //<予備>
		_PAY_INFO2		PayUmatan[6];         //<馬単払戻>
		_PAY_INFO3		PaySanrenpuku[3];     //<3連複払戻>
		_PAY_INFO4		PaySanrentan[6];      //<3連単払戻>
		char		crlf[2];                  //レコード区切り
}				JV_HR_PAY;


//****** ５．票数（全掛式）****************************************
//<票数情報１ 単・複・枠>
typedef struct
{
		char		Umaban[2];                //馬番
		char		Hyo[11];                  //票数
		char		Ninki[2];                 //人気
}				_HYO_INFO1;


//<票数情報２ 馬連・ワイド・馬単>
typedef struct
{
		char		Kumi[4];                  //組番     
		char		Hyo[11];                  //票数
		char		Ninki[3];                 //人気
}				_HYO_INFO2;


//<票数情報３ ３連複票数>
typedef struct
{
		char		Kumi[6];                  //組番     
		char		Hyo[11];                  //票数
		char		Ninki[3];                 //人気
}				_HYO_INFO3;


//<票数情報４ ３連単票数>
typedef struct
{
		char		Kumi[6];                  //組番     
		char		Hyo[11];                  //票数
		char		Ninki[4];                 //人気
}				_HYO_INFO4;


typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		HatubaiFlag[7][1];        //発売フラグ　
		char		FukuChakuBaraiKey[1];     //複勝着払キー
		char		HenkanUma[28][1];         //返還馬番情報[馬番01～28]
		char		HenkanWaku[8][1];         //返還枠番情報[枠番1～8]
		char		HenkanDoWaku[8][1];       //返還同枠情報[枠番1～8]
		_HYO_INFO1	HyoTansyo[28];            //<単勝票数>
		_HYO_INFO1	HyoFukusyo[28];           //<複勝票数>
		_HYO_INFO1	HyoWakuren[36];           //<枠連票数>
		_HYO_INFO2	HyoUmaren[153];           //<馬連票数>
		_HYO_INFO2	HyoWide[153];             //<ワイド票数>
		_HYO_INFO2	HyoUmatan[306];           //<馬単票数>
		_HYO_INFO3	HyoSanrenpuku[816];       //<3連複票数>
		char		HyoTotal[14][11];         //票数合計
		char		crlf[2];                  //レコード区切り
}				JV_H1_HYOSU_ZENKAKE;

typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		HatubaiFlag[1];        	  //発売フラグ
		char		HenkanUma[18][1];         //返還馬番情報[馬番01～18]
		_HYO_INFO4	HyoSanrentan[4896];       //<3連単票数>
		char		HyoTotal[2][11];         //票数合計
		char		crlf[2];                  //レコード区切り
}				JV_H6_HYOSU_SANRENTAN;

//****** ６．オッズ（単複枠）****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		TansyoFlag[1];            //発売フラグ　単勝
		char		FukusyoFlag[1];           //発売フラグ　複勝
		char		WakurenFlag[1];           //発売フラグ　枠連
		char		FukuChakuBaraiKey[1];     //複勝着払キー

		struct _ODDS_TANSYO_INFO                  //<単勝オッズ>
                {
				char		Umaban[2];                //馬番
				char		Odds[4];                  //オッズ
				char		Ninki[2];                 //人気順
		}			OddsTansyoInfo[28];

		struct _ODDS_FUKUSYO_INFO                 //<複勝オッズ>
                {
				char		Umaban[2];                //馬番
				char		OddsLow[4];               //最低オッズ
				char		OddsHigh[4];              //最高オッズ
				char		Ninki[2];                 //人気順
		}			OddsFukusyoInfo[28];

		struct _ODDS_WAKUREN_INFO                 //<枠連オッズ>
                {
				char		Kumi[2];                  //組
				char		Odds[5];                  //オッズ
				char		Ninki[2];                 //人気順
		}			OddsWakurenInfo[36];

		char		TotalHyosuTansyo[11];     //単勝票数合計
		char		TotalHyosuFukusyo[11];    //複勝票数合計
		char		TotalHyosuWakuren[11];    //枠連票数合計
		char		crlf[2];                  //レコード区切り
}				JV_O1_ODDS_TANFUKUWAKU;


//****** ７．オッズ（馬連）****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		UmarenFlag[1];            //発売フラグ　馬連

		struct _ODDS_UMAREN_INFO                  //<馬連オッズ>
                {
			char		Kumi[4];                  //組番
			char		Odds[6];                  //オッズ
			char		Ninki[3];                 //人気順
		}			OddsUmarenInfo[153];

		char		TotalHyosuUmaren[11];     //馬連票数合計
		char		crlf[2];                  //レコード区切り
}				JV_O2_ODDS_UMAREN;

//****** ８．オッズ（ワイド）****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		WideFlag[1];              //発売フラグ ワイド

		struct _ODDS_WIDE_INFO                    //<ワイドオッズ>
                {
				char		Kumi[4];                  //組番
				char		OddsLow[5];               //最低オッズ
				char		OddsHigh[5];              //最高オッズ
				char		Ninki[3];                 //人気順
		}			OddsWideInfo[153];

		char		TotalHyosuWide[11];       //ワイド票数合計
		char		crlf[2];                  //レコード区切り
}				JV_O3_ODDS_WIDE;


//****** ９．オッズ（馬単） ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		UmatanFlag[1];            //発売フラグ　馬単

		struct _ODDS_UMATAN_INFO                  //<馬単オッズ>
                {
				char		Kumi[4];                  //組番
				char		Odds[6];                  //オッズ
				char		Ninki[3];                 //人気順
		}			OddsUmatanInfo[306];

		char		TotalHyosuUmatan[11];     //馬単票数合計
		char		crlf[2];                  //レコード区切り
}				JV_O4_ODDS_UMATAN;


//****** １０．オッズ（３連複）****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		SanrenpukuFlag[1];        //発売フラグ　3連複

		struct _ODDS_SANREN_INFO                  //<3連複オッズ>
                {
				char		Kumi[6];              //組番
				char		Odds[6];              //オッズ
				char		Ninki[3];             //人気順
		}			OddsSanrenInfo[816];

		char		TotalHyosuSanrenpuku[11]; //3連複票数合計
		char		crlf[2];                  //レコード区切り
}				JV_O5_ODDS_SANREN;


//****** １０－１．オッズ（３連単）****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		TorokuTosu[2];            //登録頭数
		char		SyussoTosu[2];            //出走頭数
		char		SanrentanFlag[1];         //発売フラグ　3連単

		struct _ODDS_SANRENTAN_INFO           //<3連単オッズ>
                {
				char		Kumi[6];              //組番
				char		Odds[7];              //オッズ
				char		Ninki[4];             //人気順
		}			OddsSanrentanInfo[4896];

		char		TotalHyosuSanrentan[11];  //3連単票数合計
		char		crlf[2];                  //レコード区切り
}				JV_O6_ODDS_SANRENTAN;


//****** １１．競走馬マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;            //<レコードヘッダー>
		char		KettoNum[10];             //血統登録番号
		char		DelKubun[1];              //競走馬抹消区分
		_YMD		RegDate;                  //競走馬登録年月日
		_YMD		DelDate;                  //競走馬抹消年月日
		_YMD		BirthDate;                //生年月日
		char		Bamei[36];                //馬名
		char		BameiKana[36];            //馬名半角カナ
		char		BameiEng[60];             //馬名欧字
		char		ZaikyuFlag[1];            //JRA施設在きゅうフラグ
		char		Reserved[19];             //予備
		char		UmaKigoCD[2];             //馬記号コード
		char		SexCD[1];                 //性別コード
		char		HinsyuCD[1];              //品種コード
		char		KeiroCD[2];               //毛色コード

		struct _KETTO3_INFO                       //<３代血統情報>
                {
				char		HansyokuNum[10];           //繁殖登録番号
				char		Bamei[36];                //馬名
		}			Ketto3Info[14];

		char		TozaiCD[1];               //東西所属コード
		char		ChokyosiCode[5];          //調教師コード
		char		ChokyosiRyakusyo[8];      //調教師名略称
		char		Syotai[20];               //招待地域名
		char		BreederCode[8];           //生産者コード
		char		BreederName[72];          //生産者名
		char		SanchiName[20];           //産地名
		char		BanusiCode[6];            //馬主コード
		char		BanusiName[64];           //馬主名
		char		RuikeiHonsyoHeiti[9];     //平地本賞金累計
		char		RuikeiHonsyoSyogai[9];    //障害本賞金累計
		char		RuikeiFukaHeichi[9];      //平地付加賞金累計
		char		RuikeiFukaSyogai[9];      //障害付加賞金累計
		char		RuikeiSyutokuHeichi[9];   //平地収得賞金累計
		char		RuikeiSyutokuSyogai[9];   //障害収得賞金累計
		_CHAKUKAISU3_INFO		ChakuSogo;               //総合着回数
		_CHAKUKAISU3_INFO		ChakuChuo;               //中央合計着回数
		_CHAKUKAISU3_INFO		ChakuKaisuBa[7];         //馬場別着回数
		_CHAKUKAISU3_INFO		ChakuKaisuJyotai[12];    //馬場状態別着回数
		_CHAKUKAISU3_INFO		ChakuKaisuKyori[6];      //距離別着回数
		char		Kyakusitu[4][3];          //脚質傾向
		char		RaceCount[3];             //登録レース数
		char		crlf[2];                  //レコード区切り
}				JV_UM_UMA;


//****** １２．騎手マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		KisyuCode[5];             //騎手コード
		char		DelKubun[1];              //騎手抹消区分
		_YMD		IssueDate;                //騎手免許交付年月日
		_YMD		DelDate;                  //騎手免許抹消年月日
		_YMD		BirthDate;                //生年月日
		char		KisyuName[34];            //騎手名漢字
		char		reserved[34];             //予備
		char		KisyuNameKana[30];        //騎手名半角カナ
		char		KisyuRyakusyo[8];         //騎手名略称
		char		KisyuNameEng[80];         //騎手名欧字
		char		SexCD[1];                 //性別区分
		char		SikakuCD[1];              //騎乗資格コード
		char		MinaraiCD[1];             //騎手見習コード
		char		TozaiCD[1];               //騎手東西所属コード
		char		Syotai[20];               //招待地域名
		char		ChokyosiCode[5];          //所属調教師コード
		char		ChokyosiRyakusyo[8];      //所属調教師名略称

		struct _HATUKIJYO_INFO                    //<初騎乗情報>
                {
				_RACE_ID		Hatukijyoid;      //年月日場回日R
				char		SyussoTosu[2];            //出走頭数
				char		KettoNum[10];             //血統登録番号
				char		Bamei[36];                //馬名
				char		KakuteiJyuni[2];          //確定着順
				char		IJyoCD[1];                //異常区分コード
		}			HatuKiJyo[2];
		

		struct _HATUSYORI_INFO                    //<初勝利情報>
                {
				_RACE_ID	Hatusyoriid;              //年月日場回日R
				char		SyussoTosu[2];            //出走頭数
				char		KettoNum[10];             //血統登録番号
				char		Bamei[36];                //馬名
		}				HatuSyori[2];

		_SAIKIN_JYUSYO_INFO     SaikinJyusyo[3];      //<最近重賞勝利情報>
		_HON_ZEN_RUIKEISEI_INFO	HonZenRuikei[3];      //<本年・前年・累計成績情報>
		char		crlf[2];           //レコード区切り
}				JV_KS_KISYU;


//****** １３．調教師マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		ChokyosiCode[5];          //調教師コード
		char		DelKubun[1];              //調教師抹消区分
		_YMD		IssueDate;                //調教師免許交付年月日
		_YMD		DelDate;                  //調教師免許抹消年月日
		_YMD		BirthDate;                //生年月日
		char		ChokyosiName[34];         //調教師名漢字
		char		ChokyosiNameKana[30];     //調教師名半角カナ
		char		ChokyosiRyakusyo[8];      //調教師名略称
		char		ChokyosiNameEng[80];      //調教師名欧字
		char		SexCD[1];                 //性別区分
		char		TozaiCD[1];               //調教師東西所属コード
		char		Syotai[20];               //招待地域名
		_SAIKIN_JYUSYO_INFO     SaikinJyusyo[3];  //<最近重賞勝利情報>
		_HON_ZEN_RUIKEISEI_INFO HonZenRuikei[3];  //<本年・前年・累計成績情報>
		char		crlf[2];                  //レコード区切り
}				JV_CH_CHOKYOSI;


//******１４．生産者マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		BreederCode[8];           //生産者コード
		char		BreederName_Co[72];       //生産者名(法人格有)
		char		BreederName[72];          //生産者名(法人格無)
		char		BreederNameKana[72];      //生産者名半角カナ
		char		BreederNameEng[168];      //生産者名欧字
		char		Address[20];              //生産者住所自治省名
		_SEI_RUIKEI_INFO        HonRuikei[2];     //<本年・累計成績情報>
		char		crlf[2];                  //レコード区切り
}				JV_BR_BREEDER;


//****** １５．馬主マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		BanusiCode[6];            //馬主コード
		char		BanusiName_Co[64];           //馬主名(法人格有)
		char		BanusiName[64];           //馬主名(法人格無)
		char		BanusiNameKana[50];       //馬主名半角カナ
		char		BanusiNameEng[100];       //馬主名欧字
		char		Fukusyoku[60];            //服色標示
		_SEI_RUIKEI_INFO        HonRuikei[2];     //<本年・累計成績情報>
		char		crlf[2];                  //レコード区切り
}				JV_BN_BANUSI;


//****** １６．繁殖馬マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		HansyokuNum[10];           //繁殖登録番号
		char		reserved[8];              //予備
		char		KettoNum[10];             //血統登録番号
		char		DelKubun[1];              //繁殖馬抹消区分(現在は予備として使用)
		char		Bamei[36];                //馬名
		char		BameiKana[40];            //馬名半角カナ
		char		BameiEng[80];             //馬名欧字
		char		BirthYear[4];             //生年
		char		SexCD[1];                 //性別コード
		char		HinsyuCD[1];              //品種コード
		char		KeiroCD[2];               //毛色コード
		char		HansyokuMochiKubun[1];    //繁殖馬持込区分
		char		ImportYear[4];            //輸入年
		char		SanchiName[20];           //産地名
		char		HansyokuFNum[10];          //父馬繁殖登録番号
		char		HansyokuMNum[10];          //母馬繁殖登録番号
		char		crlf[2];                  //レコード区切り
}				JV_HN_HANSYOKU;


//****** １７．産駒マスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		KettoNum[10];             //血統登録番号
		_YMD		BirthDate;                //生年月日
		char		SexCD[1];                 //性別コード
		char		HinsyuCD[1];              //品種コード
		char		KeiroCD[2];               //毛色コード
		char		SankuMochiKubun[1];       //産駒持込区分
		char		ImportYear[4];            //輸入年
		char		BreederCode[8];           //生産者コード
		char		SanchiName[20];           //産地名
		char		HansyokuNum[14][10];       //3代血統 繁殖登録番号
		char		crlf[2];                  //レコード区切り
}				JV_SK_SANKU;


//****** １８．レコードマスタ ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		RecInfoKubun[1];          //レコード識別区分
		_RACE_ID	id;                       //<競走識別情報１>
		char		TokuNum[4];               //特別競走番号
		char		Hondai[60];               //競走名本題
		char		GradeCD[1];               //グレードコード
		char		SyubetuCD[2];             //競走種別コード
		char		Kyori[4];                 //距離
		char		TrackCD[2];               //トラックコード
		char		RecKubun[1];              //レコード区分
		char		RecTime[4];               //レコードタイム
		_TENKO_BABA_INFO		TenkoBaba;        //天候・馬場状態

		struct _RECUMA_INFO                       //<レコード保持馬情報>
                {
				char		KettoNum[10];             //血統登録番号
				char		Bamei[36];                //馬名
				char		UmaKigoCD[2];             //馬記号コード
				char		SexCD[1];                 //性別コード
				char		ChokyosiCode[5];          //調教師コード
				char		ChokyosiName[34];         //調教師名
				char		Futan[3];                 //負担重量
				char		KisyuCode[5];             //騎手コード
				char		KisyuName[34];            //騎手名
		}			RecUmaInfo[3];
		
		char		crlf[2];                   //レコード区切り
}				JV_RC_RECORD;


//****** １９．坂路調教 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		TresenKubun[1];           //トレセン区分
		_YMD		ChokyoDate;               //調教年月日
		char		ChokyoTime[4];            //調教時刻
		char		KettoNum[10];             //血統登録番号
		char		HaronTime4[4];            //4ハロンタイム合計[800M-0M]
		char		LapTime4[3];              //ラップタイム[800M-600M]
		char		HaronTime3[4];            //3ハロンタイム合計[600M-0M]
		char		LapTime3[3];              //ラップタイム[600M-400M]
		char		HaronTime2[4];            //2ハロンタイム合計[400M-0M]
		char		LapTime2[3];              //ラップタイム[400M-200M]
		char		LapTime1[3];              //ラップタイム[200M-0M]
		char		crlf[2];                  //レコード区切り
}				JV_HC_HANRO;


//****** ２０．馬体重 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分

		struct _BATAIJYU_INFO                     //<馬体重情報>
                {
				char		Umaban[2];                //馬番
				char		Bamei[36];                //馬名
				char		BaTaijyu[3];              //馬体重
				char		ZogenFugo[1];             //増減符号
				char		ZogenSa[3];               //増減差
		}				BataijyuInfo[18];

		char		crlf[2];                   //レコード区切り
}				JV_WH_BATAIJYU;


//****** ２１．天候馬場状態 ******************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID2	id;                       //<競走識別情報２>
		_MDHM		HappyoTime;               //発表月日時分
		char		HenkoID[1];               //変更識別
		_TENKO_BABA_INFO		TenkoBaba;        //現在状態情報
		_TENKO_BABA_INFO		TenkoBabaBefore;  //変更前状態情報
		char		crlf[2];                  //レコード区切り
}				JV_WE_WEATHER;


//****** ２２．出走取消・競争除外 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		Umaban[2];                //馬番
		char		Bamei[36];                //馬名
		char		JiyuKubun[3];             //事由区分
		char		crlf[2];                  //レコード区切り
}				JV_AV_INFO;


//************ ２３．騎手変更 **************************************** 
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		char		Umaban[2];                //馬番
		char		Bamei[36];                //馬名
		_JC_INFO 	JCInfoAfter;              //<変更後情報>
		_JC_INFO	JCInfoBefore;             //<変更前情報>
		char		crlf[2];                  //レコード区切り
}				JV_JC_INFO;


//************ ２３－１．発走時刻変更 **************************************** 
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		_TC_INFO 	TCInfoAfter;              //<変更後情報>
		_TC_INFO	TCInfoBefore;             //<変更前情報>
		char		crlf[2];                  //レコード区切り
}				JV_TC_INFO;


//************ ２３－２．コース変更 **************************************** 
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_MDHM		HappyoTime;               //発表月日時分
		_CC_INFO 	CCInfoAfter;              //<変更後情報>
		_CC_INFO	CCInfoBefore;             //<変更前情報>
		char		JiyuCD[1];                //事由コード
		char		crlf[2];                  //レコード区切り
}				JV_CC_INFO;


//****** ２４．データマイニング予想************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_HM		MakeHM;                   //データ作成時分

		struct _DM_INFO                           //<マイニング予想>
                {
				char		Umaban[2];                //馬番
				char		DMTime[5];                //予想走破タイム
				char		DMGosaP[4];               //予想誤差[信頼度]＋
				char		DMGosaM[4];               //予想誤差[信頼度]－
		}			DMInfo[18];

		char		crlf[2];                   //レコード区切り
}				JV_DM_INFO;


//****** ２５．開催スケジュール************************************
typedef struct
{
               _RECORD_ID       head;                     //<レコードヘッダー>
               _RACE_ID2        id;                       //<競走識別情報２>
               char             YoubiCD[1];               //曜日コード

               struct _JYUSYO_INFO                        //<重賞案内>
               { 
                                char            TokuNum[4];             //特別競走番号
                                char            Hondai[60];              //競走名本題
                                char            Ryakusyo10[20];          //競走名略称10字
                                char            Ryakusyo6[12];           //競走名略称6字
                                char            Ryakusyo3[6];            //競走名略称3字
                                char            Nkai[3];                 //重賞回次[第N回]
                                char            GradeCD[1];              //グレードコード
                                char            SyubetuCD[2];            //競走種別コード
                                char            KigoCD[3];               //競走記号コード
                                char            JyuryoCD[1];             //重量種別コード
                                char            Kyori[4];                //距離
                                char            TrackCD[2];              //トラックコード
               }                        JyusyoInfo[3];
 
               char             crlf[2];                  //レコード区切り
}				JV_YS_SCHEDULE;


//****** ２６．競走馬市場取引価格 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		KettoNum[10];             //血統登録番号
		char		HansyokuFNum[10];          //父馬繁殖登録番号
		char		HansyokuMNum[10];          //母馬繁殖登録番号
		char		BirthYear[4];             //生年
		char		SaleCode[6];              //主催者・市場コード
		char		SaleHostName[40];         //主催者名称
		char		SaleName[80];             //市場の名称
		_YMD		FromDate;                 //市場の開催期間(開始日)
		_YMD		ToDate;                   //市場の開催期間(終了日)
		char		Barei[1];                 //取引時の競走馬の年齢
		char		Price[10];                //取引価格
		char		crlf[2];                  //レコード区切り
}				JV_HS_SALE;


//****** ２７．馬名の意味由来 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		KettoNum[10];             //血統登録番号
		char		Bamei[36];                //馬名
		char		Origin[64];               //馬名の意味由来
		char		crlf[2];                  //レコード区切り
}				JV_HY_BAMEIORIGIN;

//****** ２８．出走別着度数 ****************************************

//<出走別着度数 競走馬情報>
typedef struct
{
		char					KettoNum [10];            //血統登録番号
		char					Bamei[36];                //馬名
		char					RuikeiHonsyoHeiti[9];     //平地本賞金累計
		char					RuikeiHonsyoSyogai[9];    //障害本賞金累計
		char					RuikeiFukaHeichi[9];      //平地付加賞金累計
		char					RuikeiFukaSyogai[9];      //障害付加賞金累計
		char					RuikeiSyutokuHeichi[9];   //平地収得賞金累計
		char					RuikeiSyutokuSyogai[9];   //障害収得賞金累計
		_CHAKUKAISU3_INFO		ChakuSogo;                //総合着回数
		_CHAKUKAISU3_INFO		ChakuChuo;                //中央合計着回数
		_CHAKUKAISU3_INFO		ChakuKaisuBa[7];          //馬場別着回数
		_CHAKUKAISU3_INFO		ChakuKaisuJyotai[12];     //馬場状態別着回数
		_CHAKUKAISU3_INFO		ChakuKaisuSibaKyori[9];   //芝距離別着回数
		_CHAKUKAISU3_INFO		ChakuKaisuDirtKyori[9];   //ダート距離別着回数
		_CHAKUKAISU3_INFO		ChakuKaisuJyoSiba[10];    //競馬場別芝着回数
		_CHAKUKAISU3_INFO		ChakuKaisuJyoDirt[10];    //競馬場別ダート着回数
		_CHAKUKAISU3_INFO		ChakuKaisuJyoSyogai[10];  //競馬場別障害着回数
		char					Kyakusitu[4][3];          //脚質傾向
		char					RaceCount[3];             //登録レース数
}				JV_CK_UMA;

//<出走別着度数 本年・累計成績情報>
typedef struct
{
		char					SetYear[4];               //設定年
		char					HonSyokinHeichi[10];      //平地本賞金合計
		char					HonSyokinSyogai[10];      //障害本賞金合計
		char					FukaSyokinHeichi[10];     //平地付加賞金合計
		char					FukaSyokinSyogai[10];     //障害付加賞金合計
		_CHAKUKAISU5_INFO		ChakuKaisuSiba;           //芝着回数
		_CHAKUKAISU5_INFO		ChakuKaisuDirt;           //ダート着回数
		_CHAKUKAISU4_INFO		ChakuKaisuSyogai;         //障害着回数
		_CHAKUKAISU4_INFO		ChakuKaisuSibaKyori[9];   //芝距離別着回数
		_CHAKUKAISU4_INFO		ChakuKaisuDirtKyori[9];   //ダート距離別着回数
		_CHAKUKAISU4_INFO		ChakuKaisuJyoSiba[10];    //競馬場別芝着回数
		_CHAKUKAISU4_INFO		ChakuKaisuJyoDirt[10];    //競馬場別ダート着回数
		_CHAKUKAISU3_INFO		ChakuKaisuJyoSyogai[10];  //競馬場別障害着回数
}				_CK_HON_RUIKEISEI_INFO;

//<出走別着度数 騎手情報>
typedef struct
{
		char					KisyuCode[5];             //騎手コード
		char					KisyuName[34];            //騎手名漢字
		_CK_HON_RUIKEISEI_INFO	HonRuikei[2];             //<本年・累計成績情報>
}				JV_CK_KISYU;

//<出走別着度数 調教師情報>
typedef struct
{
		char					ChokyosiCode[5];          //調教師コード
		char					ChokyosiName[34];         //調教師名漢字
		_CK_HON_RUIKEISEI_INFO	HonRuikei[2];             //<本年・累計成績情報>
}				JV_CK_CHOKYOSI;

//<出走別着度数 馬主情報>
typedef struct
{
		char					BanusiCode[6];            //馬主コード
		char					BanusiName_Co[64];        //馬主名（法人格有）
		char					BanusiName[64];           //馬主名（法人格無）
		_SEI_RUIKEI_INFO		HonRuikei[2];             //<本年・累計成績情報>
}				JV_CK_BANUSI;

//<出走別着度数 生産者情報>
typedef struct
{
		char					BreederCode[8];           //生産者コード
		char					BreederName_Co[72];       //生産者名（法人格有）
		char					BreederName[72];          //生産者名（法人格無）
		_SEI_RUIKEI_INFO		HonRuikei[2];             //<本年・累計成績情報>
}				JV_CK_BREEDER;

typedef struct
{
		_RECORD_ID				head;                     //<レコードヘッダー>
		_RACE_ID				id;                       //<競走識別情報１>
		JV_CK_UMA				UmaChaku;                 //<出走別着度数 競走馬情報>
		JV_CK_KISYU				KisyuChaku;               //<出走別着度数 騎手情報>
		JV_CK_CHOKYOSI			ChokyoChaku;              //<出走別着度数 調教師情報>
		JV_CK_BANUSI			BanusiChaku;              //<出走別着度数 馬主情報>
		JV_CK_BREEDER			BreederChaku;             //<出走別着度数 生産者情報>
		char					crlf[2];                  //レコード区切り
}				JV_CK_CHAKU;

//****** ２９．系統情報 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		HansyokuNum[10];           //繁殖登録番号
		char		KeitoId[30];              //系統ID
		char		KeitoName[36];            //系統名
		char		KeitoEx[6800];            //系統説明
		char		crlf[2];                  //レコード区切り
}				JV_BT_KEITO;

//****** ３０．コース情報 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		JyoCD[2];                 //競馬場コード
		char		Kyori[4];                 //距離
		char		TrackCD[2];               //トラックコード
		_YMD		KaishuDate;               //コース改修年月日
		char		CourseEx[6800];           //コース説明
		char		crlf[2];                  //レコード区切り
}				JV_CS_COURSE;

//****** ３１．対戦型データマイニング予想 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		_HM		MakeHM;                       //データ作成時分

		struct _TM_INFO                       //<マイニング予想>
                {
				char		Umaban[2];        //馬番
				char		TMScore[4];       //予測スコア
		}			TMInfo[18];

		char		crlf[2];                  //レコード区切り
}				JV_TM_INFO;

//****** ３２．重勝式(WIN5) ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_YMD		KaisaiDate;               //開催年月日
		char		reserved1[2];             //予備

		struct _WF_RACE_INFO
                {
                char JyoCD[2];                //競馬場コード
                char Kaiji[2];                //開催回[第N回]
                char Nichiji[2];              //開催日目[N日目]
                char RaceNum[2];              //レース番号
		}			WFRaceInfo[5];

		char 		reserved2[6];             //予備
		char		Hatsubai_Hyo[11];         //重勝式発売票数

		struct _WF_YUKO_HYO_INFO
                {
                char Yuko_Hyo[11];            //有効票数
		}			WFYukoHyoInfo[5];

		char		HenkanFlag[1];            //返還フラグ
		char		FuseiritsuFlag[1];        //不成立フラグ
		char		TekichunashiFlag[1];      //的中無フラグ
		char		COShoki[15];              //キャリーオーバー金額初期
		char		COZanDaka[15];            //キャリーオーバー金額残高

		struct _WF_PAY_INFO
                {
                char Kumiban[10];             //組番
                char Pay[9];                  //重勝式払戻金
                char Tekichu_Hyo[10];         //的中票数
		}			WFPayInfo[243];

		char		crlf[2];                  //レコード区切り
}				JV_WF_INFO;

//****** ３３．競走馬除外情報 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		_RACE_ID	id;                       //<競走識別情報１>
		char		KettoNum[10];             //血統登録番号
		char		Bamei[36];                //馬名
		char		ShutsubaTohyoJun[3];      //出馬投票受付順番
		char		ShussoKubun[1];           //出走区分
		char		JogaiJotaiKubun[1];       //除外状態区分
		char		crlf[2];                  //レコード区切り
}				JV_JG_JOGAIBA;

//****** ３４．ウッドチップ調教 ****************************************
typedef struct
{
		_RECORD_ID	head;                     //<レコードヘッダー>
		char		TresenKubun[1];           //トレセン区分
		_YMD		ChokyoDate;               //調教年月日
		char		ChokyoTime[4];            //調教時刻
		char		KettoNum[10];             //血統登録番号
		char		Course[1];                // コース
		char		BabaAround[1];            // 馬場周り
		char		reserved[1];              // 予備
		char		HaronTime10[4];           //10ハロンタイム合計[2000M-0M]
		char		LapTime10[3];             //ラップタイム[2000M-1800M]
		char		HaronTime9[4];            //9ハロンタイム合計[1800M-0M]
		char		LapTime9[3];              //ラップタイム[1800M-1600M]
		char		HaronTime8[4];            //8ハロンタイム合計[1600M-0M]
		char		LapTime8[3];              //ラップタイム[1600M-1400M]
		char		HaronTime7[4];            //7ハロンタイム合計[1400M-0M]
		char		LapTime7[3];              //ラップタイム[1400M-1200M]
		char		HaronTime6[4];            //6ハロンタイム合計[1200M-0M]
		char		LapTime6[3];              //ラップタイム[1200M-1000M]
		char		HaronTime5[4];            //5ハロンタイム合計[1000M-0M]
		char		LapTime5[3];              //ラップタイム[1000M-800M]
		char		HaronTime4[4];            //4ハロンタイム合計[800M-0M]
		char		LapTime4[3];              //ラップタイム[800M-600M]
		char		HaronTime3[4];            //3ハロンタイム合計[600M-0M]
		char		LapTime3[3];              //ラップタイム[600M-400M]
		char		HaronTime2[4];            //2ハロンタイム合計[400M-0M]
		char		LapTime2[3];              //ラップタイム[400M-200M]
		char		LapTime1[3];              //ラップタイム[200M-0M]
		char		crlf[2];                  //レコード区切り
}				JV_WC_WOOD;


#endif	// __JV_DATA_STRUCT

