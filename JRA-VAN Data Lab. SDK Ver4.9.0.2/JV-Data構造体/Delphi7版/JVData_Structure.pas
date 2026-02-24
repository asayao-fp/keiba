unit JVData_Structure;

interface
uses
  SysUtils;
type

    //========================================================================
    //  JRA-VAN Data Lab. JV-Data構造体
    //
    //
    //   作成: JRA-VAN ソフトウェア工房 
    //
    //========================================================================
    //   (C) JRA SYSTEM SERVICE CO.,LTD. 2007 All rights reserved
    //========================================================================
    

    ////////// 共通構造体 //////////


    //<年月日>
    _YMD = record
        Year    : array[0..3] of Char;                          //年
        Month   : array[0..1] of Char;                          //月
        Day     : array[0..1] of Char;                          //日
    end;


    //<時分秒>
    _HMS = record
        Hour   : array[0..1] of Char;                           //時
        Minute : array[0..1] of Char;                           //分
        Second : array[0..1] of Char;                           //秒
    end;


    //<時分>
    _HM = record
        Hour   : array[0..1] of Char;                           //時
        Minute : array[0..1] of Char;                           //分
    end;


    //<月日時分>
    _MDHM = record
        Month  : array[0..1] of Char;                           //月
        Day    : array[0..1] of Char;                           //日
        Hour   : array[0..1] of Char;                           //時
        Minute : array[0..1] of Char;                           //分
    end;


    //<レコードヘッダ>
    _RECORD_ID = record
        RecordSpec : array[0..1] of Char;                       //レコード種別
        DataKubun  : array[0..0] of Char;                       //データ区分
        MakeDate   : _YMD;                                      //データ作成年月日
    end;


    //<競走識別情報１>
    _RACE_ID = record
        Year     : array[0..3]  of Char;                        //開催年
        MonthDay : array[0..3]  of Char;                        //開催月日
        JyoCD    : array[0..1]  of Char;                        //競馬場コード
        Kaiji    : array[0..1]  of Char;                        //開催回[第N回]
        Nichiji  : array[0..1]  of Char;                        //開催日目[N日目]
        RaceNum  : array[0..1]  of Char;                        //レース番号
    end;


    //<競走識別情報２>
    _RACE_ID2 = record
        Year     : array[0..3]  of Char;                        //開催年
        MonthDay : array[0..3]  of Char;                        //開催月日
        JyoCD    : array[0..1]  of Char;                        //競馬場コード
        Kaiji    : array[0..1]  of Char;                        //開催回[第N回]
        Nichiji  : array[0..1]  of Char;                        //開催日目[N日目]
    end;


    //<着回数（サイズ3byte）>
    _CHAKUKAISU3_INFO = record
        Chakukaisu : array[0..5,0..2] of Char;                  //着回数
    end;

    //<着回数（サイズ4byte）>
    _CHAKUKAISU4_INFO = record
        Chakukaisu : array[0..5,0..3] of Char;                  //着回数
    end;

    //<着回数（サイズ5byte）>
    _CHAKUKAISU5_INFO = record
        Chakukaisu : array[0..5,0..4] of Char;                  //着回数
    end;

    //<着回数（サイズ6byte）>
    _CHAKUKAISU6_INFO = record
        Chakukaisu : array[0..5,0..5] of Char;                  //着回数
    end;


    //<本年・累計成績情報>
    _SEI_RUIKEI_INFO = record
        SetYear        : array[0..3]      of Char;              //設定年
        HonSyokinTotal : array[0..9]      of Char;              //本賞金合計
        FukaSyokin     : array[0..9]      of Char;              //付加賞金合計
        ChakuKaisu     : array[0..5,0..5] of Char;              //着回数
    end;


    //<最近重賞勝利情報>
    _SAIKIN_JYUSYO_INFO = record
        SaikinJyusyoid : _RACE_ID;                               //<年月日場回日R>
        Hondai         : array[0..59] of Char;                  //競走名本題
        Ryakusyo10     : array[0..19] of Char;                  //競走名略称10字
        Ryakusyo6      : array[0..11] of Char;                  //競走名略称6字
        Ryakusyo3      : array[0..5]  of Char;                  //競走名略称3字
        GradeCD        : array[0..0]  of Char;                  //グレードコード
        SyussoTosu     : array[0..1]  of Char;                  //出走頭数
        KettoNum       : array[0..9]  of Char;                  //血統登録番号
        Bamei          : array[0..35] of Char;                  //馬名
    end;


    //<本年・前年・累計成績情報>
    _HON_ZEN_RUIKEISEI_INFO = record
        SetYear            : array[0..3] of Char;               //設定年
        HonSyokinHeichi    : array[0..9] of Char;               //平地本賞金合計
        HonSyokinSyogai    : array[0..9] of Char;               //障害本賞金合計
        FukaSyokinHeichi   : array[0..9] of Char;               //平地付加賞金合計
        FukaSyokinSyogai   : array[0..9] of Char;               //障害付加賞金合計
        ChakuKaisuHeichi   : _CHAKUKAISU6_INFO;                 //平地着回数
        ChakuKaisuSyogai   : _CHAKUKAISU6_INFO;                 //障害着回数
        ChakuKaisuJyo      : array[0..19]of _CHAKUKAISU6_INFO;  //競馬場別着回数
        ChakuKaisuKyori    : array[0..5] of _CHAKUKAISU6_INFO;  //距離別着回数
    end;


    //<レース情報>
    _RACE_INFO = record
        YoubiCD    : array[0..0]   of Char;                     //曜日コード
        TokuNum    : array[0..3]   of Char;                     //特別競走番号
        Hondai     : array[0..59]  of Char;                     //競走名本題
        Fukudai    : array[0..59]  of Char;                     //競走名副題
        Kakko      : array[0..59]  of Char;                     //競走名カッコ内
        HondaiEng  : array[0..119] of Char;                     //競走名本題欧字
        FukudaiEng : array[0..119] of Char;                     //競走名副題欧字
        KakkoEng   : array[0..119] of Char;                     //競走名カッコ内欧字
        Ryakusyo10 : array[0..19]  of Char;                     //競走名略称１０字
        Ryakusyo6  : array[0..11]  of Char;                     //競走名略称６字
        Ryakusyo3  : array[0..5]   of Char;                     //競走名略称３字
        Kubun      : array[0..0]   of Char;                     //競走名区分
        Nkai       : array[0..2]   of Char;                     //重賞回次[第N回]
    end;


    //<天候・馬場状態>
    _TENKO_BABA_INFO = record
        TenkoCD    : array[0..0]  of Char;                      //天候コード
        SibaBabaCD : array[0..0]  of Char;                      //芝馬場状態コード
        DirtBabaCD : array[0..0]  of Char;                      //ダート馬場状態コード
    end;


    //<競走条件コード>
    _RACE_JYOKEN = record
        SyubetuCD   : array[0..1]      of Char;                 //競走種別コード
        KigoCD      : array[0..2]      of Char;                 //競走記号コード
        JyuryoCD    : array[0..0]      of Char;                 //重量種別コード
        JyokenCD    : array[0..4,0..2] of Char;                 //競走条件コード
    end;


	//<騎手変更情報>
    _JC_INFO = record
        Futan       : array[0..2]      of Char;                 //負担重量
        KisyuCode   : array[0..4]      of Char;                 //騎手コード
        KisyuName   : array[0..33]     of Char;                 //騎手名
        MinaraiCD   : array[0..0]      of Char;                 //騎手見習コード
    end;

	//<発走時刻変更情報>
    _TC_INFO = record
        Ji       : array[0..1]      of Char;                 //時間
        Fun      : array[0..1]      of Char;                 //分
    end;
  
	//<コース変更情報>
    _CC_INFO = record
        Kyori       : array[0..3]      of Char;                 //距離
        TruckCD      : array[0..1]      of Char;                 //トラック
    end;


    ////////// データ構造体 //////////


    //****** １．特別登録馬 ****************************************

    JV_TK_TOKUUMA = record
        head          : _RECORD_ID;                              //<レコードヘッダー>
        id            : _RACE_ID;                                //<競走識別情報１>
        RaceInfo      : _RACE_INFO;                              //<レース情報>
        GradeCD       : array[0..0] of Char;                    //グレードコード
        JyokenInfo    : _RACE_JYOKEN;                            //<競走条件コード>
        Kyori         : array[0..3] of Char;                    //距離
        TrackCD       : array[0..1] of Char;                    //トラックコード
        CourseKubunCD : array[0..1] of Char;                    //コース区分
        HandiDate     : _YMD;                                   //ハンデ発表日
        TorokuTosu    : array[0..2] of Char;                    //登録頭数

        TokuUmaInfo : array[0..299] of record                   //<登録馬毎情報>
            Num               : array[0..2]   of Char;          //連番
            KettoNum          : array[0..9]   of Char;          //血統登録番号
            Bamei             : array[0..35]  of Char;          //馬名
            UmaKigoCD         : array[0..1]   of Char;          //馬記号コード
            SexCD             : array[0..0]   of Char;          //性別コード
            TozaiCD           : array[0..0]   of Char;          //調教師東西所属コード
            ChokyosiCode      : array[0..4]   of Char;          //調教師コード
            ChokyosiRyakusyo  : array[0..7]   of Char;          //調教師名略称
            Futan             : array[0..2]   of Char;          //負担重量
            Koryu             : array[0..0]   of Char;          //交流区分
        end;

        crlf          : array[0..1] of Char;                    //レコード区切

    end;


    //****** ２．レース詳細 ****************************************

    JV_RA_RACE = record
        head                  : _RECORD_ID;                     //<レコードヘッダー>
        id                    : _RACE_ID;                       //<競走識別情報１>
        RaceInfo              : _RACE_INFO;                     //<レース情報>
        GradeCD               : array[0..0] of Char;            //グレードコード
        GradeCDBefore         : array[0..0] of Char;            //変更前グレードコード
        JyokenInfo            : _RACE_JYOKEN;                   //<競走条件コード>
        JyokenName            : array[0..59]of Char;            //競走条件名称
        Kyori                 : array[0..3] of Char;            //距離
        KyoriBefore           : array[0..3] of Char;            //変更前距離
        TrackCD               : array[0..1] of Char;            //トラックコード
        TrackCDBefore         : array[0..1] of Char;            //変更前トラックコード
        CourseKubunCD         : array[0..1] of Char;            //コース区分
        CourseKubunCDBefore   : array[0..1] of Char;            //変更前コース区分
        Honsyokin             : array[0..6,0..7] of Char;       //本賞金
        HonsyokinBefore       : array[0..4,0..7] of Char;       //変更前本賞金
        Fukasyokin            : array[0..4,0..7] of Char;       //付加賞金
        FukasyokinBefore      : array[0..2,0..7] of Char;       //変更前付加賞金
        HassoTime             : array[0..3] of Char;            //発走時刻
        HassoTimeBefore       : array[0..3] of Char;            //変更前発走時刻
        TorokuTosu            : array[0..1] of Char;            //登録頭数
        SyussoTosu            : array[0..1] of Char;            //出走頭数
        NyusenTosu            : array[0..1] of Char;            //入線頭数
        TenkoBaba             :_TENKO_BABA_INFO;                //天候・馬場状態コード
        LapTime               : array[0..24,0..2] of Char;      //ラップタイム
        SyogaiMileTime        : array[0..3] of Char;            //障害マイルタイム
        HaronTimeS3           : array[0..2] of Char;            //前３ハロンタイム
        HaronTimeS4           : array[0..2] of Char;            //前４ハロンタイム
        HaronTimeL3           : array[0..2] of Char;            //後３ハロンタイム
        HaronTimeL4           : array[0..2] of Char;            //後４ハロンタイム

        CornerInfo : array[0..3] of record                      //<コーナー通過順位>
            Corner            : array[0..0] of Char;            //コーナー
            Syukaisu          : array[0..0] of Char;            //周回数
            Jyuni             : array[0..69] of Char;           //各通過順位
        end;

        RecordUpKubun         : array[0..0] of Char;            //レコード更新区分
        crlf                  : array[0..1] of Char;            //レコード区切り
    end;


    //****** ３．馬毎レース情報 ****************************************

    JV_SE_RACE_UMA = record
        head         : _RECORD_ID;                              //<レコードヘッダー>
        id           : _RACE_ID;                                //<競走識別情報１>
        Wakuban      : array[0..0] of Char;                     //枠番
        Umaban       : array[0..1] of Char;                     //馬番
        KettoNum     : array[0..9] of Char;                     //血統登録番号
        Bamei        : array[0..35]of Char;                     //馬名
        UmaKigoCD    : array[0..1] of Char;                     //馬記号コード
        SexCD        : array[0..0] of Char;                     //性別コード
        HinsyuCD     : array[0..0] of Char;                     //品種コード
        KeiroCD      : array[0..1] of Char;                     //毛色コード
        Barei        : array[0..1] of Char;                     //馬齢
        TozaiCD      : array[0..0] of Char;                     //東西所属コード
        ChokyosiCode : array[0..4] of Char;                     //調教師コード
        ChokyosiRyakusyo : array[0..7] of Char;                 //調教師名略称
        BanusiCode   : array[0..5] of Char;                     //馬主コード
        BanusiName   : array[0..63] of Char;                    //馬主名
        Fukusyoku    : array[0..59] of Char;                    //服色標示
        reserved1    : array[0..59] of Char;                    //予備
        Futan        : array[0..2] of Char;                     //負担重量
        FutanBefore  : array[0..2] of Char;                     //変更前負担重量
        Blinker      : array[0..0] of Char;                     //ブリンカー使用区分
        reserved2    : array[0..0] of Char;                     //予備
        KisyuCode    : array[0..4] of Char;                     //騎手コード
        KisyuCodeBefore : array[0..4] of Char;                  //変更前騎手コード
        KisyuRyakusyo : array[0..7] of Char;                    //騎手名略称
        KisyuRyakusyoBefore : array[0..7] of Char;              //変更前騎手名略称
        MinaraiCD    : array[0..0] of Char;                     //騎手見習コード
        MinaraiCDBefore : array[0..0] of Char;                  //変更前騎手見習コード
        BaTaijyu     : array[0..2] of Char;                     //馬体重
        ZogenFugo    : array[0..0] of Char;                     //増減符号
        ZogenSa      : array[0..2] of Char;                     //増減差
        IJyoCD       : array[0..0] of Char;                     //異常区分コード
        NyusenJyuni  : array[0..1] of Char;                     //入線順位
        KakuteiJyuni : array[0..1] of Char;                     //確定着順
        DochakuKubun : array[0..0] of Char;                     //同着区分
        DochakuTosu  : array[0..0] of Char;                     //同着頭数
        Time         : array[0..3] of Char;                     //走破タイム
        ChakusaCD    : array[0..2] of Char;                     //着差コード
        ChakusaCDP   : array[0..2] of Char;                     //+着差コード
        ChakusaCDPP  : array[0..2] of Char;                     //++着差コード
        Jyuni1c      : array[0..1] of Char;                     //1コーナーでの順位
        Jyuni2c      : array[0..1] of Char;                     //2コーナーでの順位
        Jyuni3c      : array[0..1] of Char;                     //3コーナーでの順位
        Jyuni4c      : array[0..1] of Char;                     //4コーナーでの順位
        Odds         : array[0..3] of Char;                     //単勝オッズ
        Ninki        : array[0..1] of Char;                     //単勝人気順
        Honsyokin    : array[0..7] of Char;                     //獲得本賞金
        Fukasyokin   : array[0..7] of Char;                     //獲得付加賞金
        reserved3    : array[0..2] of Char;                     //予備
        reserved4    : array[0..2] of Char;                     //予備
        HaronTimeL4  : array[0..2] of Char;                     //後４ハロンタイム
        HaronTimeL3  : array[0..2] of Char;                     //後３ハロンタイム
        
        ChakuUmaInfo : array[0..2] of record                    //<1着馬(相手馬)情報>
            KettoNum : array[0..9]  of Char;                    //血統登録番号
            Bamei    : array[0..35] of Char;                    //馬名
        end;
        
        TimeDiff     : array[0..3] of Char;                     //タイム差
        RecordUpKubun : array[0..0] of Char;                    //レコード更新区分
        DMKubun      : array[0..0] of Char;                     //マイニング区分
        DMTime       : array[0..4] of Char;                     //マイニング予想走破タイム
        DMGosaP      : array[0..3] of Char;                     //予測誤差(信頼度)＋
        DMGosaM      : array[0..3] of Char;                     //予測誤差(信頼度)－
        DMJyuni      : array[0..1] of Char;                     //マイニング予想順位
        KyakusituKubun : array[0..0] of Char;                   //今回レース脚質判定
        crlf         : array[0..1] of Char;                     //レコード区切り
        end;


    //****** ４．払戻 ****************************************

    //<払戻情報１ 単・複・枠>
    _PAY_INFO1 = record
        Umaban      : array[0..1] of Char;                      //馬番
        Pay         : array[0..8] of Char;                      //払戻金
        Ninki       : array[0..1] of Char;                      //人気順 
    end;

    //<払戻情報２ 馬連・ワイド・予備・馬単>
    _PAY_INFO2 = record
        Kumi        : array[0..3] of Char;                      //組番
        Pay         : array[0..8] of Char;                      //払戻金
        Ninki       : array[0..2] of Char;                      //人気順
    end;


    //<払戻情報３ ３連複>
    _PAY_INFO3 = record
        Kumi        : array[0..5] of Char;                      //組番
        Pay         : array[0..8] of Char;                      //払戻金
        Ninki       : array[0..2] of Char;                      //人気順 
    end;


    //<払戻情報４ ３連単>
    _PAY_INFO4 = record
        Kumi        : array[0..5] of Char;                      //組番
        Pay         : array[0..8] of Char;                      //払戻金
        Ninki       : array[0..3] of Char;                      //人気順
    end;

    JV_HR_PAY = record
        head             : _RECORD_ID;                          //<レコードヘッダー>
        id               : _RACE_ID;                            //<競走識別情報１>
        TorokuTosu       : array[0..1] of Char;                 //登録頭数
        SyussoTosu       : array[0..1] of Char;                 //出走頭数
        FuseirituFlag    : array[0..8,0..0] of Char;            //不成立フラグ
        TokubaraiFlag    : array[0..8,0..0] of Char;            //特払フラグ
        HenkanFlag       : array[0..8,0..0] of Char;            //返還フラグ
        HenkanUma        : array[0..27,0..0] of Char;           //返還馬番情報(馬番01～28)
        HenkanWaku       : array[0..7,0..0] of Char;            //返還枠番情報(枠番1～8)
        HenkanDoWaku     : array[0..7,0..0] of Char;            //返還同枠情報(枠番1～8)
        
        PayTansyo        : array[0..2] of _PAY_INFO1;           //<単勝払戻>
        PayFukusyo       : array[0..4] of _PAY_INFO1;           //<複勝払戻>
        PayWakuren       : array[0..2] of _PAY_INFO1;           //<枠連払戻>
        PayUmaren        : array[0..2] of _PAY_INFO2;           //<馬連払戻>
        PayWide          : array[0..6] of _PAY_INFO2;           //<ワイド払戻>
        PayReserved1     : array[0..2] of _PAY_INFO2;           //<予備>
        PayUmatan        : array[0..5] of _PAY_INFO2;           //<馬単払戻>
        PaySanrenpuku    : array[0..2] of _PAY_INFO3;           //<3連複払戻>
        PaySanrentan     : array[0..5] of _PAY_INFO4;           //<3連単払戻>
        crlf             : array[0..1] of Char;                 //レコード区切り
    end;


    //****** ５．票数（全掛式）****************************************

    //<票数情報１ 単・複・枠>
    _HYO_INFO1 = record
        Umaban           : array[0..1] of Char;                 //馬番
        Hyo              : array[0..10] of Char;                //票数
        Ninki            : array[0..1] of Char;                 //人気
    end;


    //<票数情報２ 馬連・ワイド・馬単>
    _HYO_INFO2 = record
        Kumi             : array[0..3] of Char;                 //組番
        Hyo              : array[0..10] of Char;                //票数
        Ninki            : array[0..2] of Char;                 //人気
    end;


    //<票数情報３ ３連複票数>
    _HYO_INFO3 = record
        Kumi             : array[0..5] of Char;                 //組番
        Hyo              : array[0..10] of Char;                //票数
        Ninki            : array[0..2] of Char;                 //人気
    end;



    JV_H1_HYOSU_ZENKAKE = record
        head :_RECORD_ID;                                       //<レコードヘッダー>
        id : _RACE_ID;                                          //<競走識別情報１>
        TorokuTosu        : array[0..1] of Char;                //登録頭数
        SyussoTosu        : array[0..1] of Char;                //出走頭数
        HatubaiFlag       : array[0..6,0..0] of Char;           //発売フラグ
        FukuChakuBaraiKey : array[0..0] of Char;                //複勝着払キー
        HenkanUma         : array[0..27,0..0] of Char;          //返還馬番情報(馬番01～28)
        HenkanWaku        : array[0..7,0..0] of Char;           //返還枠番情報(枠番1～8)
        HenkanDoWaku      : array[0..7,0..0] of Char;           //返還同枠情報(枠番1～8)
        HyoTansyo         : array[0..27] of _HYO_INFO1;         //<単勝票数>
        HyoFukusyo        : array[0..27] of _HYO_INFO1;         //<複勝票数>
        HyoWakuren        : array[0..35] of _HYO_INFO1;         //<枠連票数>
        HyoUmaren         : array[0..152] of _HYO_INFO2;        //<馬連票数>
        HyoWide           : array[0..152] of _HYO_INFO2;        //<ワイド票数>
        HyoUmatan         : array[0..305] of _HYO_INFO2;        //<馬単票数>
        HyoSanrenpuku     : array[0..815] of _HYO_INFO3;        //<3連複票数>
        HyoTotal          : array[0..13,0..10] of Char;         //票数合計
        crlf              : array[0..1] of Char;                //レコード区切り
    end;


    //****** ５．票数（全掛式）****************************************

    //<票数情報６ ３連単票数>
    _HYO_INFO4 = record
        Kumi             : array[0..5] of Char;                 //組番
        Hyo              : array[0..10] of Char;                //票数
        Ninki            : array[0..3] of Char;                 //人気
    end;


    JV_H6_HYOSU_SANRENTAN = record
        head :_RECORD_ID;                                       //<レコードヘッダー>
        id : _RACE_ID;                                          //<競走識別情報１>
        TorokuTosu        : array[0..1] of Char;                //登録頭数
        SyussoTosu        : array[0..1] of Char;                //出走頭数
        HatubaiFlag       : array[0..0] of Char;           //発売フラグ
        HenkanUma         : array[0..17,0..0] of Char;          //返還馬番情報(馬番01～28)
        HyoSanrentan      : array[0..4895] of _HYO_INFO4;        //<3連複票数>
        HyoTotal          : array[0..1,0..10] of Char;         //票数合計
        crlf              : array[0..1] of Char;                //レコード区切り
    end;



    //****** ６．オッズ（単複枠）****************************************

    JV_O1_ODDS_TANFUKUWAKU = record
        head              : _RECORD_ID;                        //<レコードヘッダー>
        id                : _RACE_ID;                          //<競走識別情報１>
        HappyoTime        : _MDHM;                             //発表月日時分
        TorokuTosu        : array[0..1] of Char;               //登録頭数
        SyussoTosu        : array[0..1] of Char;               //出走頭数
        TansyoFlag        : array[0..0] of Char;               //発売フラグ 単勝
        FukusyoFlag       : array[0..0] of Char;               //発売フラグ 複勝
        WakurenFlag       : array[0..0] of Char;               //発売フラグ　枠連
        FukuChakuBaraiKey : array[0..0] of Char;               //複勝着払キー
        
        OddsTansyoInfo : array[0..27] of record                //<単勝オッズ>
            Umaban        : array[0..1]  of Char;              //馬番
            Odds          : array[0..3]  of Char;              //オッズ
            Ninki         : array[0..1]  of Char;              //人気順
        end;
        
        OddsFukusyoInfo : array[0..27] of record               //<複勝票数オッズ>
            Umaban        : array[0..1]  of Char;              //馬番
            OddsLow       : array[0..3]  of Char;              //最低オッズ
            OddsHigh      : array[0..3]  of Char;              //最高オッズ
            Ninki         : array[0..1]  of Char;              //人気順
        end;
        
        OddsWakurenInfo : array[0..35] of record               //<枠連票数オッズ>
            Kumi          : array[0..1]  of Char;              //組
            Odds          : array[0..4]  of Char;              //オッズ
            Ninki         : array[0..1]  of Char;              //人気順
        end;

        TotalHyosuTansyo  : array[0..10] of Char;              //単勝票数合計
        TotalHyosuFukusyo : array[0..10] of Char;              //複勝票数合計
        TotalHyosuWakuren : array[0..10] of Char;              //枠連票数合計
        crlf              : array[0..1]  of Char;              //レコード区切り
    end;


    //****** ７．オッズ（馬連）****************************************

    JV_O2_ODDS_UMAREN = record
        head             : _RECORD_ID;                         //<レコードヘッダー>
        id               : _RACE_ID;                           //<競走識別情報１>
        HappyoTime       : _MDHM;                              //発表月日時分
        TorokuTosu       : array[0..1] of Char;                //登録頭数
        SyussoTosu       : array[0..1] of Char;                //出走頭数
        UmarenFlag       : array[0..0] of Char;                //発売フラグ　馬連
        
        OddsUmarenInfo : array[0..152] of record               ///<馬連オッズ>
            Kumi         : array[0..3]  of Char;               //組番
            Odds         : array[0..5]  of Char;               //オッズ
            Ninki        : array[0..2]  of Char;               //人気順
        end;

        TotalHyosuUmaren : array[0..10] of Char;               //馬連票数合計
        crlf             : array[0..1]  of Char;               //レコード区切り
    end;


    //****** ８．オッズ（ワイド）****************************************
   
    JV_O3_ODDS_WIDE = record
        head           : _RECORD_ID;                          //<レコードヘッダー>
        id             : _RACE_ID;                            //<競走識別情報１>
        HappyoTime     : _MDHM;                               //発表月日時分
        TorokuTosu     : array[0..1]   of Char;               //登録頭数
        SyussoTosu     : array[0..1]   of Char;               //出走頭数
        WideFlag       : array[0..0]   of Char;               //発売フラグ　ワイド
        
        OddsWideInfo : array[0..152] of record                //<ワイドオッズ>
            Kumi       : array[0..3]   of Char;               //組番
            OddsLow    : array[0..4]   of Char;               //最低オッズ
            OddsHigh   : array[0..4]   of Char;               //最高オッズ
            Ninki      : array[0..2]   of Char;               //人気順
        end;
    
        TotalHyosuWide : array[0..10]  of Char;               //ワイド票数合計
         crlf          : array[0..1]   of Char;               //レコード区切り
    end;


    //****** ９．オッズ（馬単） ****************************************

    JV_O4_ODDS_UMATAN = record
        head              : _RECORD_ID;                       //<レコードヘッダー>
        id                : _RACE_ID;                         //<競走識別情報１>
        HappyoTime        : _MDHM;                            //発表月日時分
        TorokuTosu        : array[0..1]   of Char;            //登録頭数
        SyussoTosu        : array[0..1]   of Char;            //出走頭数
        UmatanFlag        : array[0..0]   of Char;            //発売フラグ　馬単

        OddsUmatanInfo : array[0..305] of record              //<馬単オッズ>
            Kumi          : array[0..3]   of Char;            //組番
            Odds          : array[0..5]   of Char;            //オッズ
            Ninki         : array[0..2]   of Char;            //人気順
        end;
    
        TotalHyosuUmatan  : array[0..10] of Char;             //馬単票数合計
        crlf              : array[0..1]  of Char;             //レコード区切り
    end;


    //****** １０．オッズ（３連複）****************************************
   
    JV_O5_ODDS_SANREN = record
        head           : _RECORD_ID;                         //<レコードヘッダー>
        id             : _RACE_ID;                           //<競走識別情報１>
        HappyoTime     : _MDHM;                              //発表月日時分
        TorokuTosu     : array[0..1] of Char;                //登録頭数
        SyussoTosu     : array[0..1] of Char;                //出走頭数
        SanrenpukuFlag : array[0..0] of Char;                //発売フラグ　3連複
        
        OddsSanrenInfo : array[0..815] of record             //<3連複オッズ>
            Kumi  : array[0..5] of Char;                     //組番
            Odds  : array[0..5] of Char;                     //オッズ
            Ninki : array[0..2] of Char;                     //人気順
        end;

        TotalHyosuSanrenpuku : array[0..10] of Char;         //3連複票数合計
        crlf : array[0..1] of Char;                          //レコード区切り
    end;
    
    //****** １１．オッズ（３連単）****************************************
   
    JV_O6_ODDS_SANRENTAN = record
        head           : _RECORD_ID;                         //<レコードヘッダー>
        id             : _RACE_ID;                           //<競走識別情報１>
        HappyoTime     : _MDHM;                              //発表月日時分
        TorokuTosu     : array[0..1] of Char;                //登録頭数
        SyussoTosu     : array[0..1] of Char;                //出走頭数
        SanrentanFlag : array[0..0] of Char;                //発売フラグ　3連単
        
        OddsSanrentanInfo : array[0..4895] of record         //<3連単オッズ>
            Kumi  : array[0..5] of Char;                     //組番
            Odds  : array[0..6] of Char;                     //オッズ
            Ninki : array[0..3] of Char;                     //人気順
        end;

        TotalHyosuSanrentan : array[0..10] of Char;         //3連複票数合計
        crlf : array[0..1] of Char;                          //レコード区切り
    end;


    //****** １２．競走馬マスタ ****************************************
  
    JV_UM_UMA = record
        head             : _RECORD_ID;                       //<レコードヘッダー>
        KettoNum         : array[0..9]  of Char;             //血統登録番号
        DelKubun         : array[0..0]  of Char;             //競走馬抹消区分
        RegDate          : _YMD;                             //競走馬登録年月日
        DelDate          : _YMD;                             //競走馬抹消年月日
        BirthDate        : _YMD;                             //生年月日
        Bamei            : array[0..35] of Char;             //馬名
        BameiKana        : array[0..35] of Char;             //馬名半角カナ
        BameiEng         : array[0..59] of Char;             //馬名欧字
        ZaikyuFlag       : array[0..0]  of Char;             //JRA施設在きゅうフラグ
        Reserved         : array[0..18] of Char;             //予備
        UmaKigoCD        : array[0..1]  of Char;             //馬記号コード
        SexCD            : array[0..0]  of Char;             //性別コード
        HinsyuCD         : array[0..0]  of Char;             //品種コード
        KeiroCD          : array[0..1]  of Char;             //毛色コード
        
        Ketto3Info       : array[0..13] of record            //<3代血統情報>
            HansyokuNum  : array[0..9]  of Char;             //繁殖登録番号
            Bamei        : array[0..35] of Char;             //馬名
        end;
        
        TozaiCD          : array[0..0]  of Char;             //東西所属コード
        ChokyosiCode     : array[0..4]  of Char;             //調教師コード
        ChokyosiRyakusyo : array[0..7]  of Char;             //調教師名略称
        Syotai           : array[0..19] of Char;             //招待地域名
        BreederCode      : array[0..7]  of Char;             //生産者コード
        BreederName      : array[0..71] of Char;             //生産者名
        SanchiName       : array[0..19] of Char;             //産地名
        BanusiCode       : array[0..5]  of Char;             //馬主コード
        BanusiName       : array[0..63] of Char;             //馬主名
        RuikeiHonsyoHeiti  : array[0..8]  of Char;           //平地本賞金累計
        RuikeiHonsyoSyogai : array[0..8]  of Char;           //障害本賞金累計
        RuikeiFukaHeichi : array[0..8]    of Char;           //平地付加賞金累計
        RuikeiFukaSyogai : array[0..8]    of Char;           //障害付加賞金累計
        RuikeiSyutokuHeichi : array[0..8] of Char;           //平地収得賞金累計
        RuikeiSyutokuSyogai : array[0..8] of Char;           //障害収得賞金累計
        ChakuSogo        : _CHAKUKAISU3_INFO;               //総合着回数
        ChakuChuo        : _CHAKUKAISU3_INFO;               //中央合計着回数
        ChakuKaisuBa     : array[0..6]    of _CHAKUKAISU3_INFO;   //馬場別着回数
        ChakuKaisuJyotai : array[0..11]   of _CHAKUKAISU3_INFO;   //馬場状態別着回数
        ChakuKaisuKyori  : array[0..5]    of _CHAKUKAISU3_INFO;   //距離別着回数
        Kyakusitu        : array[0..3,0..2]    of Char;      //脚質傾向
        RaceCount        : array[0..2]    of Char;           //登録レース数
        crlf             : array[0..1]    of Char;           //レコード区切り
    end;


    //****** １３．騎手マスタ ****************************************

    JV_KS_KISYU = record
        head                 :_RECORD_ID;                    //<レコードヘッダー>
        KisyuCode            : array[0..4]  of Char;         //騎手コード
        DelKubun             : array[0..0]  of Char;         //騎手抹消区分
        IssueDate            : _YMD;                         //騎手免許交付年月日
        DelDate              : _YMD;                         //騎手免許抹消年月日
        BirthDate            : _YMD;                         //生年月日
        KisyuName            : array[0..33] of Char;         //騎手名漢字
        reserved             : array[0..33] of Char;         //予備
        KisyuNameKana        : array[0..29] of Char;         //騎手名半角カナ
        KisyuRyakusyo        : array[0..7]  of Char;         //騎手名略称
        KisyuNameEng         : array[0..79] of Char;         //騎手名欧字
        SexCD                : array[0..0]  of Char;         //性別区分
        SikakuCD             : array[0..0]  of Char;         //騎乗資格コード
        MinaraiCD            : array[0..0]  of Char;         //騎手見習コード
        TozaiCD              : array[0..0]  of Char;         //騎手東西所属コード
        Syotai               : array[0..19] of Char;         //招待地域名
        ChokyosiCode         : array[0..4]  of Char;         //所属調教師コード
        ChokyosiRyakusyo     : array[0..7]  of Char;         //所属調教師名略称

        HatuKiJyo            : array[0..1]  of record        //<初騎乗情報>
            Hatukijyoid      : _RACE_ID;                     //年月日場回日R
            SyussoTosu       : array[0..1]  of Char;         //出走頭数
            KettoNum         : array[0..9]  of Char;         //血統登録番号
            Bamei            : array[0..35] of Char;         //馬名
            KakuteiJyuni     : array[0..1]  of Char;         //確定着順
            IJyoCD           : array[0..0]  of Char;         //異常区分コード
        end;

        HatuSyori            : array[0..1]  of record        //<初勝利情報>
            Hatusyoriid      : _RACE_ID;                     //年月日場回日R
            SyussoTosu       : array[0..1]  of Char;         //出走頭数
            KettoNum         : array[0..9]  of Char;         //血統登録番号
            Bamei            : array[0..35] of Char;         //馬名
        end;

        SaikinJyusyo         : array[0..2]  of _SAIKIN_JYUSYO_INFO;     //<最近重賞勝利情報>
        HonZenRuikei         : array[0..2]  of _HON_ZEN_RUIKEISEI_INFO; //<本年・前年・累計成績情報>
        crlf                 : array[0..1]  of Char;                    //レコード区切り
    end;


    //****** １４．調教師マスタ ****************************************

    JV_CH_CHOKYOSI = record
        head            : _RECORD_ID;                         //<レコードヘッダー>
        ChokyosiCode    : array[0..4] of Char;                //調教師コード
        DelKubun        : array[0..0] of Char;                //調教師抹消区分
        IssueDate       : _YMD;                //調教師免許交付年月日
        DelDate         : _YMD;                //調教師免許抹消年月日
        BirthDate       : _YMD;                //生年月日
        ChokyosiName    : array[0..33] of Char;               //調教師名漢字
        ChokyosiNameKana : array[0..29] of Char;              //調教師名半角カナ
        ChokyosiRyakusyo : array[0..7] of Char;               //調教師名略称
        ChokyosiNameEng : array[0..79] of Char;               //調教師名欧字
        SexCD           : array[0..0] of Char;                //性別区分
        TozaiCD         : array[0..0] of Char;                //調教師東西所属コード
        Syotai          : array[0..19] of Char;               //招待地域名
        SaikinJyusyo    : array[0..2] of _SAIKIN_JYUSYO_INFO;     //<最近重賞勝利情報>
        HonZenRuikei    : array[0..2] of _HON_ZEN_RUIKEISEI_INFO; //<本年・前年・累計成績情報>
        crlf            : array[0..1] of Char;                //レコード区切り
    end;


    //******１５．生産者マスタ ****************************************

    JV_BR_BREEDER = record
        head            :_RECORD_ID;                          //<レコードヘッダー>
        BreederCode     : array[0..7]   of Char;              //生産者コード
        BreederName_Co  : array[0..71] of Char;               //生産者名(法人格有)
        BreederName     : array[0..71] of Char;               //生産者名(法人格無)
        BreederNameKana : array[0..71]  of Char;              //生産者名半角カナ
        BreederNameEng  : array[0..167] of Char;              //生産者名欧字
        Address         : array[0..19]   of Char;             //生産者住所自治省名
        HonRuikei       : array[0..1]   of _SEI_RUIKEI_INFO;  //<本年・累計成績情報>
        crlf            : array[0..1]   of Char;              //レコード区切り
    end;


    //****** １６．馬主マスタ ****************************************

    JV_BN_BANUSI = record
        head :_RECORD_ID;                                     //<レコードヘッダー>
        BanusiCode     : array[0..5]  of Char;                //馬主コード
        BanusiName_Co  : array[0..63] of Char;                //馬主名(法人格有)
        BanusiName     : array[0..63] of Char;                //馬主名(法人格無)
        BanusiNameKana : array[0..49] of Char;                //馬主名半角カナ
        BanusiNameEng  : array[0..99] of Char;                //馬主名欧字
        Fukusyoku      : array[0..59] of Char;                //服色標示
        HonRuikei      : array[0..1]  of _SEI_RUIKEI_INFO;    //<本年・累計成績情報>
        crlf           : array[0..1]  of Char;                //レコード区切り
    end;


    //****** １７．繁殖馬マスタ ****************************************

    JV_HN_HANSYOKU = record
        head        : _RECORD_ID;                             //<レコードヘッダー>
        HansyokuNum : array[0..9] of Char;                    //繁殖登録番号
        reserved    : array[0..7] of Char;                    //予備
        KettoNum    : array[0..9] of Char;                    //血統登録番号
        DelKubun    : array[0..0] of Char;                    //繁殖馬抹消区分(現在は予備として使用)
        Bamei       : array[0..35] of Char;                   //馬名
        BameiKana   : array[0..39] of Char;                   //馬名半角カナ
        BameiEng    : array[0..79] of Char;                   //馬名欧字
        BirthYear   : array[0..3] of Char;                    //生年
        SexCD       : array[0..0] of Char;                    //性別コード
        HinsyuCD    : array[0..0] of Char;                    //品種コード
        KeiroCD     : array[0..1] of Char;                    //毛色コード
        HansyokuMochiKubun : array[0..0] of Char;             //繁殖馬持込区分
        ImportYear  : array[0..3] of Char;                    //輸入年
        SanchiName  : array[0..19] of Char;                   //産地名
        HansyokuFNum : array[0..9] of Char;                   //父馬繁殖登録番号
        HansyokuMNum : array[0..9] of Char;                   //母馬繁殖登録番号
        crlf        : array[0..1] of Char;                    //レコード区切り
    end;


    //****** １８．産駒マスタ ****************************************

    JV_SK_SANKU = record
        head            : _RECORD_ID;                         //<レコードヘッダー>
        KettoNum        : array[0..9] of Char;                //血統登録番号
        BirthDate       : _YMD;                               //生年月日
        SexCD           : array[0..0] of Char;                //性別コード
        HinsyuCD        : array[0..0] of Char;                //品種コード
        KeiroCD         : array[0..1] of Char;                //毛色コード
        SankuMochiKubun : array[0..0] of Char;                //産駒持込区分
        ImportYear      : array[0..3] of Char;                //輸入年
        BreederCode     : array[0..7] of Char;                //生産者コード
        SanchiName      : array[0..19] of Char;               //産地名
        HansyokuNum     : array[0..13,0..9] of Char;          //3代血統 繁殖登録番号
        crlf            : array[0..1] of Char;                //レコード区切り
    end;


    //****** １９．レコードマスタ ****************************************
    
    JV_RC_RECORD = record
        head             : _RECORD_ID;                        //<レコードヘッダー>
        RecInfoKubun     : array[0..0] of Char;               //レコード識別区分
        id               : _RACE_ID;                          //<競走識別情報１>
        TokuNum          : array[0..3] of Char;               //特別競走番号
        Hondai           : array[0..59] of Char;              //競走名本題
        GradeCD          : array[0..0] of Char;               //グレードコード
        SyubetuCD        : array[0..1] of Char;               //競走種別コード
        Kyori            : array[0..3] of Char;               //距離
        TrackCD          : array[0..1] of Char;               //トラックコード
        RecKubun         : array[0..0] of Char;               //レコード区分
        RecTime          : array[0..3] of Char;               //レコードタイム
        TenkoBaba        : _TENKO_BABA_INFO;                  //天候・馬場状態

        RecUmaInfo : array[0..2] of record                    //<レコード保持馬情報>
            KettoNum     : array[0..9]  of Char;              //血統登録番号
            Bamei        : array[0..35] of Char;              //馬名
            UmaKigoCD    : array[0..1]  of Char;              //馬記号コード
            SexCD        : array[0..0]  of Char;              //性別コード
            ChokyosiCode : array[0..4]  of Char;              //調教師コード
            ChokyosiName : array[0..33] of Char;              //調教師名
            Futan        : array[0..2]  of Char;              //負担重量
            KisyuCode    : array[0..4]  of Char;              //騎手コード
            KisyuName    : array[0..33] of Char;              //騎手名
        end;

        crlf            : array[0..1] of Char;                //レコード区切り
    end;


    //****** ２０．坂路調教 ****************************************

    JV_HC_HANRO = record
        head        : _RECORD_ID;                             //<レコードヘッダー>
        TresenKubun : array[0..0] of Char;                    //トレセン区分
        ChokyoDate  : _YMD;                                   //調教年月日
        ChokyoTime  : array[0..3] of Char;                    //調教時刻
        KettoNum    : array[0..9] of Char;                    //血統登録番号
        HaronTime4  : array[0..3] of Char;                    //4ハロンタイム合計(800M-0M)
        LapTime4    : array[0..2] of Char;                    //ラップタイム(800M-600M)
        HaronTime3  : array[0..3] of Char;                    //3ハロンタイム合計(600M-0M)
        LapTime3    : array[0..2] of Char;                    //ラップタイム(600M-400M)
        HaronTime2  : array[0..3] of Char;                    //2ハロンタイム合計(400M-0M)
        LapTime2    : array[0..2] of Char;                    //ラップタイム(400M-200M)
        LapTime1    : array[0..2] of Char;                    //ラップタイム(200M-0M)
        crlf        : array[0..1] of Char;                    //レコード区切り
    end;


    //****** ２０．馬体重 ****************************************
  
     JV_WH_BATAIJYU = record
        head               : _RECORD_ID;                      //<レコードヘッダー>
        id                 : _RACE_ID;                        //<競走識別情報１>
        HappyoTime         : _MDHM;                           //発表月日時分

        BataijyuInfo : array[0..17] of record                 //<馬体重情報>
            Umaban         : array[0..1]  of Char;            //馬番
            Bamei          : array[0..35] of Char;            //馬名
            BaTaijyu       : array[0..2]  of Char;            //馬体重
            ZogenFugo      : array[0..0]  of Char;            //増減符号
            ZogenSa        : array[0..2]  of Char;            //増減差
        end;

        crlf               : array[0..1] of Char;             //レコード区切り
    end;


    //****** ２１．天候馬場状態 ******************************************

    JV_WE_WEATHER = record
        head            : _RECORD_ID;                         //<レコードヘッダー>
        id              : _RACE_ID2;                          //<競走識別情報２>
        HappyoTime      : _MDHM;                              //発表月日時分
        HenkoID         : array[0..0] of Char;                //変更識別
        TenkoBaba       : _TENKO_BABA_INFO;                   //現在状態情報
        TenkoBabaBefore : _TENKO_BABA_INFO;                   //変更前状態情報
        crlf            : array[0..1] of Char;                //レコード区切り
    end;


    //****** ２２．出走取消・競争除外 ****************************************

     JV_AV_INFO = record 
        head        : _RECORD_ID;                             //<レコードヘッダー>
        id          : _RACE_ID;                               //<競走識別情報１>
        HappyoTime  : _MDHM;                                  //発表月日時分
        Umaban      : array[0..1]  of Char;                   //馬番
        Bamei       : array[0..35] of Char;                   //馬名
        JiyuKubun   : array[0..2]  of Char;                   //事由区分
        crlf        : array[0..1]  of Char;                   //レコード区切り
    end;


    //************ ２３．騎手変更 ****************************************

     JV_JC_INFO = record                                      //<変更情報>
        head          : _RECORD_ID;                           //<レコードヘッダー>
        id            : _RACE_ID;                             //<競走識別情報１>
        HappyoTime    : _MDHM;                                //発表月日時分
        Umaban        : array[0..1]  of Char;                 //馬番
        Bamei         : array[0..35] of Char;                 //馬名
        JCInfoAfter   : _JC_INFO;                             //<変更後情報>
        JCInfoBefore  : _JC_INFO;                             //<変更前情報>
        crlf          : array[0..1] of Char;                  //レコード区切り
    end;

    //************ ２３．発走時刻変更 ****************************************

     JV_TC_INFO = record                                      //<変更情報>
        head          : _RECORD_ID;                           //<レコードヘッダー>
        id            : _RACE_ID;                             //<競走識別情報１>
        HappyoTime    : _MDHM;                                //発表月日時分
        TCInfoAfter   : _TC_INFO;                             //変更後情報
        TCInfoBefore  : _TC_INFO;                             //変更前情報
        crlf          : array[0..1] of Char;                  //レコード区切り
    end;
    
    //************ ２３．コース変更 ****************************************

     JV_CC_INFO = record                                      //<変更情報>
        head          : _RECORD_ID;                           //<レコードヘッダー>
        id            : _RACE_ID;                             //<競走識別情報１>
        HappyoTime    : _MDHM;                                //発表月日時分
        CCInfoAfter   : _CC_INFO;                             //<変更後情報>
        CCInfoBefore  : _CC_INFO;                             //<変更前情報>
        JiyuKubun     : array[0..0]  of Char;                 //事由区分
        crlf          : array[0..1] of Char;                  //レコード区切り
    end;    
    //****** ２４．データマイニング予想************************************

    JV_DM_INFO = record                                       //<マイニング予想>
        head         : _RECORD_ID;                            //<レコードヘッダー>
        id           : _RACE_ID;                              //<競走識別情報１>
        MakeHM       : _HM;                                   //データ作成時分

        DMInfo : array[0..17] of record                       //<マイニング予想>
            Umaban   : array[0..1]  of Char;                  //馬番
            DMTime   : array[0..4]  of Char;                  //予想走破タイム
            DMGosaP  : array[0..3]  of Char;                  //予想誤差(信頼度)＋
            DMGosaM  : array[0..3]  of Char;                  //予想誤差(信頼度)－
        end;

        crlf         : array[0..1]  of Char;                  //レコード区切り
    end;


    //****** ２５．開催スケジュール************************************

    JV_YS_SCHEDULE = record
        head         : _RECORD_ID;                           //<レコードヘッダー>
        id           : _RACE_ID2;                            //<競走識別情報２>
        YoubiCD      : array[0..0] of Char;                  //曜日コード

        JyusyoInfo : array[0..2] of record                   //<重賞案内>
            TokuNum  : array[0..3] of Char;                  //特別競走番号
            Hondai   : array[0..59] of Char;                 //競走名本題
            Ryakusyo10   : array[0..19] of Char;             //競走名略称10字
            Ryakusyo6    : array[0..11] of Char;             //競走名略称6字
            Ryakusyo3    : array[0..5] of Char;              //競走名略称3字
            Nkai         : array[0..2] of Char;              //重賞回次[第N回]
            GradeCD      : array[0..0] of Char;              //グレードコード
            SyubetuCD    : array[0..1] of Char;              //競走種別コード
            KigoCD   : array[0..2] of Char;                  //競走記号コード
            JyuryoCD : array[0..0] of Char;                  //重量種別コード
            Kyori    : array[0..3] of Char;                  //距離
            TrackCD  : array[0..1] of Char;                  //トラックコード
        end;
 
        crlf         : array[0..1] of Char;                  //レコード区切り
     end;


    //****** ２６．競走馬市場取引価格 ****************************************

    JV_HS_SALE = record
        head         : _RECORD_ID;                           //<レコードヘッダー>
        KettoNum     : array[0..9] of Char;                  //血統登録番号
        HansyokuFNum : array[0..9] of Char;                  //父馬繁殖登録番号
        HansyokuMNum : array[0..9] of Char;                  //母馬繁殖登録番号
        BirthYear    : array[0..3] of Char;                  //生年
        SaleCode     : array[0..5] of Char;                  //主催者・市場コード
        SaleHostName : array[0..39] of Char;                 //主催者名称
        SaleName     : array[0..79] of Char;                 //市場の名称
        FromDate     : _YMD;                                 //市場の開催期間(開始日)
        ToDate       : _YMD;                                 //市場の開催期間(終了日)
        Barei        : array[0..0] of Char;                  //取引時の競走馬の年齢
        Price        : array[0..9] of Char;                  //取引価格
        crlf         : array[0..0] of Char;                  //レコード区切り
     end;

    //****** ２７．馬名の意味由来 ****************************************

    JV_HY_BAMEIORIGIN = record
        head         : _RECORD_ID;                           //<レコードヘッダー>
        KettoNum     : array[0..9] of Char;                  //血統登録番号
        Bamei        : array[0..35] of Char;                 //馬名
        Origin       : array[0..63] of Char;                 //馬名の意味由来
        crlf         : array[0..0] of Char;                  //レコード区切り
     end;

    //****** ２８．出走別着度数 ****************************************

    //<出走別着度数 競走馬情報>

    JV_CK_UMA = record
        KettoNum            : array[0..9] of Char;                   //血統登録番号
        Bamei               : array[0..35] of Char;                  //馬名
        RuikeiHonsyoHeiti   : array[0..8] of Char;                   //平地本賞金累計
        RuikeiHonsyoSyogai  : array[0..8] of Char;                   //障害本賞金累計
        RuikeiFukaHeichi    : array[0..8] of Char;                   //平地付加賞金累計
        RuikeiFukaSyogai    : array[0..8] of Char;                   //障害付加賞金累計
        RuikeiSyutokuHeichi : array[0..8] of Char;                   //平地収得賞金累計
        RuikeiSyutokuSyogai : array[0..8] of Char;                   //障害収得賞金累計
        ChakuSogo           : _CHAKUKAISU3_INFO;                     //総合着回数
        ChakuChuo           : _CHAKUKAISU3_INFO;                     //中央合計着回数
        ChakuKaisuBa        : array[0..6]    of _CHAKUKAISU3_INFO;   //馬場別着回数
        ChakuKaisuJyotai    : array[0..11]   of _CHAKUKAISU3_INFO;   //馬場状態別着回数
        ChakuKaisuSibaKyori : array[0..8]    of _CHAKUKAISU3_INFO;   //芝距離別着回数
        ChakuKaisuDirtKyori : array[0..8]    of _CHAKUKAISU3_INFO;   //ダート距離別着回数
        ChakuKaisuJyoSiba   : array[0..9]    of _CHAKUKAISU3_INFO;   //競馬場別芝着回数
        ChakuKaisuJyoDirt   : array[0..9]    of _CHAKUKAISU3_INFO;   //競馬場別ダート着回数
        ChakuKaisuJyoSyogai : array[0..9]    of _CHAKUKAISU3_INFO;   //競馬場別障害着回数
        Kyakusitu           : array[0..3,0..2]    of Char;           //脚質傾向
        RaceCount           : array[0..2]    of Char;                //登録レース数
     end;

    //<出走別着度数 本年・累計成績情報>

    _CK_HON_RUIKEISEI_INFO = record
        SetYear             : array[0..3] of Char;                   //設定年
        HonSyokinHeichi     : array[0..9] of Char;                   //平地本賞金合計
        HonSyokinSyogai     : array[0..9] of Char;                   //障害本賞金合計
        FukaSyokinHeichi    : array[0..9] of Char;                   //平地付加賞金合計
        FukaSyokinSyogai    : array[0..9] of Char;                   //障害付加賞金合計
        ChakuKaisuSiba      : _CHAKUKAISU5_INFO;                     //芝着回数
        ChakuKaisuDirt      : _CHAKUKAISU5_INFO;                     //ダート着回数
        ChakuKaisuSyogai    : _CHAKUKAISU4_INFO;                     //障害着回数
        ChakuKaisuSibaKyori : array[0..8]    of _CHAKUKAISU4_INFO;   //芝距離別着回数
        ChakuKaisuDirtKyori : array[0..8]    of _CHAKUKAISU4_INFO;   //ダート距離別着回数
        ChakuKaisuJyoSiba   : array[0..9]    of _CHAKUKAISU4_INFO;   //競馬場別芝着回数
        ChakuKaisuJyoDirt   : array[0..9]    of _CHAKUKAISU4_INFO;   //競馬場別ダート着回数
        ChakuKaisuJyoSyogai : array[0..9]    of _CHAKUKAISU3_INFO;   //競馬場別障害着回数
    end;

    //<出走別着度数 騎手情報>

    JV_CK_KISYU = record
        KisyuCode            : array[0..4]  of Char;                  //騎手コード
        KisyuName            : array[0..33] of Char;                  //騎手名漢字
        HonRuikei            : array[0..1]  of _CK_HON_RUIKEISEI_INFO //<本年・累計成績情報>
    end;

    //<出走別着度数 調教師情報>

    JV_CK_CHOKYOSI = record
        ChokyosiCode      : array[0..4]  of Char;                  //調教師コード
        ChokyosiName      : array[0..33] of Char;                  //調教師名漢字
        HonRuikei         : array[0..1]  of _CK_HON_RUIKEISEI_INFO //<本年・累計成績情報>
    end;

    //<出走別着度数 馬主情報>

    JV_CK_BANUSI = record
        BanusiCode     : array[0..5]  of Char;                //馬主コード
        BanusiName_Co  : array[0..63] of Char;                //馬主名(法人格有)
        BanusiName     : array[0..63] of Char;                //馬主名(法人格無)
        HonRuikei      : array[0..1]  of _SEI_RUIKEI_INFO;    //<本年・累計成績情報>
    end;

    //<出走別着度数 生産者情報>

    JV_CK_BREEDER = record
        BreederCode     : array[0..7]  of Char;               //生産者コード
        BreederName_Co  : array[0..71] of Char;               //生産者名(法人格有)
        BreederName     : array[0..71] of Char;               //生産者名(法人格無)
        HonRuikei       : array[0..1]  of _SEI_RUIKEI_INFO;   //<本年・累計成績情報>
    end;

    // 出走別着度数

    JV_CK_CHAKU = record
        head            : _RECORD_ID;                         //<レコードヘッダー>
        id              : _RACE_ID;                           //<競走識別情報１>
        UmaChaku        : JV_CK_UMA;                          //<出走別着度数 競走馬情報>
        KisyuChaku      : JV_CK_KISYU;                        //<出走別着度数 騎手情報>
        ChokyoChaku     : JV_CK_CHOKYOSI;                     //<出走別着度数 調教師情報>
        BanusiChaku     : JV_CK_BANUSI;                       //<出走別着度数 馬主情報>
        BreederChaku    : JV_CK_BREEDER;                      //<出走別着度数 生産者情報>
        crlf            : array[0..1]   of Char;              //レコード区切り
    end;

    //****** ２９．系統情報 ********************************************
    
    JV_BT_KEITO = record
        head         : _RECORD_ID;                           //<レコードヘッダー>
        HansyokuNum  : array[0..9] of Char;                  //繁殖登録番号
        KeitoId      : array[0..29] of Char;                 //系統ID
        KeitoName    : array[0..35] of Char;                 //系統名
        KeitoEx      : array[0..6799] of Char;               //系統説明
        crlf         : array[0..1] of Char;                  //レコード区切り
     end;

    //****** ３０．コース情報 ******************************************
    
    JV_CS_COURSE = record
        head         : _RECORD_ID;                           //<レコードヘッダー>
        JyoCd        : array[0..1] of Char;                  //競馬場コード
        Kyori        : array[0..3] of Char;                  //距離
        TrackCD      : array[0..1] of Char;                  //トラックコード
        KaishuDate   : _YMD;                                 //コース改修年月日
        CourseEx     : array[0..6799] of Char;               //コース説明
        crlf         : array[0..1] of Char;                  //レコード区切り
     end;

    //****** ３１．対戦型データマイニング予想************************************

    JV_TM_INFO = record
        head         : _RECORD_ID;                            //<レコードヘッダー>
        id           : _RACE_ID;                              //<競走識別情報１>
        MakeHM       : _HM;                                   //データ作成時分

        TMInfo : array[0..17] of record                       //<マイニング予想>
            Umaban   : array[0..1]  of Char;                  //馬番
            TMScore  : array[0..3]  of Char;                  //予測スコア
        end;

        crlf         : array[0..1]  of Char;                  //レコード区切り
    end;

    //****** ３２．重勝式(WIN5)************************************

    JV_WF_INFO = record
        head             : _RECORD_ID;                       //<レコードヘッダー>
        KaisaiDate       : _YMD;                             //<開催年月日>
        reserved1        : array[0..1] of Char;              //予備

        WFRaceInfo : array[0..4] of record                   //<重勝式対象レース情報>
            JyoCD        : array[0..1] of Char;              //競馬場コード
            Kaiji        : array[0..1] of Char;              //開催回[第N回]
            Nichiji      : array[0..1] of Char;              //開催日目[N日目]
            RaceNum      : array[0..1] of Char;              //レース番号
        end;

        reserved2        : array[0..5] of Char;              //予備
        Hatsubai_Hyo     : array[0..10] of Char;             //重勝式発売票数

        WFYukoHyoInfo : array[0..4] of record                //<有効票数情報>
            Yuko_Hyo     : array[0..10] of Char;             //有効票数
        end;

        HenkanFlag       : array[0..0] of Char;              //返還フラグ
        FuseiritsuFlag   : array[0..0] of Char;              //不成立フラグ
        TekichunashiFlag : array[0..0] of Char;              //的中無フラグ
        COShoki          : array[0..14] of Char;             //キャリーオーバー金額初期
        COZanDaka        : array[0..14] of Char;             //キャリーオーバー金額残高

        WFPayInfo : array[0..242] of record                  //<重勝式払戻情報>
            Kumiban      : array[0..9] of Char;              //組番
            Pay          : array[0..8] of Char;              //重勝式払戻金
            Tekichu_Hyo  : array[0..9] of Char;              //的中票数
        end;

        crlf         : array[0..1]  of Char;                 //レコード区切り
    end;

    //****** ３３．競走馬除外情報************************************

    JV_JG_JOGAIBA = record
        head             : _RECORD_ID;                       //<レコードヘッダー>
        id               : _RACE_ID;                         //<競走識別情報１>
        KettoNum         : array[0..9] of Char;              //血統登録番号
        Bamei            : array[0..35] of Char;             //馬名
        ShutsubaTohyoJun : array[0..2] of Char;              //出馬投票受付順番
        ShussoKubun      : array[0..0] of Char;              //出走区分
        JogaiJotaiKubun  : array[0..0] of Char;              //除外状態区分

        crlf             : array[0..1]  of Char;             //レコード区切り
    end;

    //****** ３４．ウッドチップ調教 ****************************************

    JV_WC_WOOD = record
        head        : _RECORD_ID;                             //<レコードヘッダー>
        TresenKubun : array[0..0] of Char;                    //トレセン区分
        ChokyoDate  : _YMD;                                   //調教年月日
        ChokyoTime  : array[0..3] of Char;                    //調教時刻
        KettoNum    : array[0..9] of Char;                    //血統登録番号
        Course      : array[0..0] of Char;                    //コース
        BabaAround  : array[0..0] of Char;                    //馬場周り
        reserved    : array[0..0] of Char;                    //予備
        HaronTime10 : array[0..3] of Char;                    //10ハロンタイム合計(2000M-0M)
        LapTime10   : array[0..2] of Char;                    //ラップタイム(2000M-1800M)
        HaronTime9  : array[0..3] of Char;                    //9ハロンタイム合計(1800M-0M)
        LapTime9    : array[0..2] of Char;                    //ラップタイム(1800M-1600M)
        HaronTime8  : array[0..3] of Char;                    //8ハロンタイム合計(1600M-0M)
        LapTime8    : array[0..2] of Char;                    //ラップタイム(1600M-1400M)
        HaronTime7  : array[0..3] of Char;                    //7ハロンタイム合計(1400M-0M)
        LapTime7    : array[0..2] of Char;                    //ラップタイム(1400M-1200M)
        HaronTime6  : array[0..3] of Char;                    //6ハロンタイム合計(1200M-0M)
        LapTime6    : array[0..2] of Char;                    //ラップタイム(1200M-1000M)     
        HaronTime5  : array[0..3] of Char;                    //5ハロンタイム合計(1000M-0M)
        LapTime5    : array[0..2] of Char;                    //ラップタイム(1000M-800M)
        HaronTime4  : array[0..3] of Char;                    //4ハロンタイム合計(800M-0M)
        LapTime4    : array[0..2] of Char;                    //ラップタイム(800M-600M)
        HaronTime3  : array[0..3] of Char;                    //3ハロンタイム合計(600M-0M)
        LapTime3    : array[0..2] of Char;                    //ラップタイム(600M-400M)
        HaronTime2  : array[0..3] of Char;                    //2ハロンタイム合計(400M-0M)
        LapTime2    : array[0..2] of Char;                    //ラップタイム(400M-200M)
        LapTime1    : array[0..2] of Char;                    //ラップタイム(200M-0M)
        crlf        : array[0..1] of Char;                    //レコード区切り
    end;

    //***** Readしたバッファを構造体にセットする関数の宣言部*******************

    function SetDataTK(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_TK_TOKUUMA):boolean;
    function SetDataRA(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_RA_RACE):boolean;
    function SetDataSE(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_SE_RACE_UMA):boolean;
    function SetDataHR(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_HR_PAY):boolean;
    function SetDataH1(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_H1_HYOSU_ZENKAKE):boolean;
    function SetDataH6(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_H6_HYOSU_SANRENTAN):boolean;
    function SetDataO1(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_O1_ODDS_TANFUKUWAKU):boolean;
    function SetDataO2(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_O2_ODDS_UMAREN):boolean;
    function SetDataO3(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_O3_ODDS_WIDE):boolean;
    function SetDataO4(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_O4_ODDS_UMATAN):boolean;
    function SetDataO5(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_O5_ODDS_SANREN):boolean;
    function SetDataO6(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_O6_ODDS_SANRENTAN):boolean;
    function SetDataUM(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_UM_UMA):boolean;
    function SetDataKS(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_KS_KISYU):boolean;
    function SetDataCH(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_CH_CHOKYOSI):boolean;
    function SetDataBR(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_BR_BREEDER):boolean;
    function SetDataBN(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_BN_BANUSI):boolean;
    function SetDataHN(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_HN_HANSYOKU):boolean;
    function SetDataSK(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_SK_SANKU):boolean;
    function SetDataRC(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_RC_RECORD):boolean;
    function SetDataHC(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_HC_HANRO):boolean;
    function SetDataWH(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_WH_BATAIJYU):boolean;
    function SetDataWE(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_WE_WEATHER):boolean;
    function SetDataAV(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_AV_INFO):boolean;
    function SetDataJC(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_JC_INFO):boolean;
    function SetDataTC(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_TC_INFO):boolean;
    function SetDataCC(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_CC_INFO):boolean;
    function SetDataDM(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_DM_INFO):boolean;
    function SetDataYS(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_YS_SCHEDULE):boolean;
    function SetDataHS(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_HS_SALE):boolean;
    function SetDataHY(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_HY_BAMEIORIGIN):boolean;
    function SetDataCK(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_CK_CHAKU):boolean;
    function SetDataBT(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_BT_KEITO):boolean;
    function SetDataCS(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_CS_COURSE):boolean;
    function SetDataTM(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_TM_INFO):boolean;
    function SetDataWF(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_WF_INFO):boolean;
    function SetDataJG(var Buff:WideString ; BuffSize:Integer ; var mbuf :JV_JG_JOGAIBA):boolean;

implementation

// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataTK(var Buff : WideString ;BuffSize :Integer;  var mbuf:JV_TK_TOKUUMA):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataTK := true;
except
    SetdataTK := false;
end;
end;

// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataRA(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_RA_RACE):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataRA := true;
except
    SetdataRA := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataSE(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_SE_RACE_UMA):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataSE := true;
except
    SetdataSE := false
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataHR(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_HR_PAY ):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataHR := true;
except
    SetdataHR := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataH1(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_H1_HYOSU_ZENKAKE ):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataH1 := true;
except
    SetdataH1 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataH6(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_H6_HYOSU_SANRENTAN ):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataH6 := true;
except
    SetdataH6 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataO1(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_O1_ODDS_TANFUKUWAKU):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataO1 := true;
except
    SetdataO1 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataO2(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_O2_ODDS_UMAREN):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataO2 := true;
except
    SetdataO2 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataO3(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_O3_ODDS_WIDE):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataO3 := true;
except
    SetdataO3 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataO4(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_O4_ODDS_UMATAN):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataO4 := true;
except
    SetdataO4 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataO5(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_O5_ODDS_SANREN):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataO5 := true;
except
    SetdataO5 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataO6(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_O6_ODDS_SANRENTAN):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataO6 := true;
except
    SetdataO6 := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataUM(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_UM_UMA):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataUM := true;
except
    SetdataUM := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataKS(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_KS_KISYU):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataKS := true;
except
    SetdataKS := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataCH(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_CH_CHOKYOSI):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataCH := true;
except
    SetdataCH := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataBR(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_BR_BREEDER):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataBR := true;
except
    SetdataBR := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataBN(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_BN_BANUSI):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataBN := true;
except
    SetdataBN := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataHN(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_HN_HANSYOKU):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataHN := true;
except
    SetdataHN := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataSK(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_SK_SANKU):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataSK := true;
except
    SetdataSK := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataRC(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_RC_RECORD):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataRC := true;
except
    SetdataRC := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataHC(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_HC_HANRO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataHC := true;
except
    SetdataHC := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataWH(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_WH_BATAIJYU):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataWH := true;
except
    SetdataWH := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataWE(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_WE_WEATHER):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataWE := true;
except
    SetdataWE := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataAV(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_AV_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataAV := true;
except
    SetdataAV := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataJC(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_JC_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataJC := true;
except
    SetdataJC := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataTC(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_TC_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataTC := true;
except
    SetdataTC := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataCC(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_CC_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataCC := true;
except
    SetdataCC := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataDM(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_DM_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataDM := true;
except
    SetdataDM := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataYS(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_YS_SCHEDULE):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataYS := true;
except
    SetdataYS := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataHS(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_HS_SALE):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataHS := true;
except
    SetdataHS := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataHY(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_HY_BAMEIORIGIN):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataHY := true;
except
    SetdataHY := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataCK(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_CK_CHAKU):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataCK := true;
except
    SetdataCK := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataBT(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_BT_KEITO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataBT := true;
except
    SetdataBT := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataCS(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_CS_COURSE):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataCS := true;
except
    SetdataCS := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataTM(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_TM_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataTM := true;
except
    SetdataTM := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataWF(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_WF_INFO):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataWF := true;
except
    SetdataWF := false;
end;
end;
// @(f)
//
// 機能      :　Readしたバッファを構造体にセットする
//
// 引き数    :  Buff － 文字列 , BuffSize － Int型 , mbuf － 構造体
//
// 返り値    :  True - 成功, False - 失敗
//
// 機能説明  :  Readしたバッファを構造体にセットする
//
function SetdataJG(var Buff : WideString ;BuffSize :Integer; var mbuf:JV_JG_JOGAIBA):boolean;
var
    MessageStr :String;
begin
try
    MessageStr:=Buff;
    strMove(@mbuf,pchar(MessageStr),BuffSize);
    SetdataJG := true;
except
    SetdataJG := false;
end;
end;
end.
