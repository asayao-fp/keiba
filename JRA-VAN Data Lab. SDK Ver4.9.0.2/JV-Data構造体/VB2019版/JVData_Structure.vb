Module JVLink_Stluct
    '========================================================================
    '  JRA-VAN Data Lab. JV-Data構造体
    '
    '
    '	作成: JRA-VAN ソフトウェア工房
    '	更新:                           2009年 9月 8日
    '
    '========================================================================
    '	(C) Copyright JRA SYSTEM SERVICE CO.,LTD. 2007 All rights reserved
    '========================================================================


    '''''''''''''''''''' セットデータのプログラミングパーツ '''''''''''''''''''''''''''''''''''''

    '------------------------------------------------------------------------
    '　　文字列をバイト長で切出し
    '------------------------------------------------------------------------
    '　 [引数]
    '		myByte			= 文字列
    '		strStart		= 開始位置
    '		strLength		= バイト長
    '	[戻り値]
    '		String			= 文字列
    '------------------------------------------------------------------------
    Public Function MidB2S(ByRef myByte As Byte(), _
          ByVal bSt As Long, _
          ByVal bLen As Long) As String
        '文字を任意に切出す
        MidB2S = System.Text.Encoding.GetEncoding(932).GetString(myByte, bSt - 1, bLen)
    End Function

    '------------------------------------------------------------------------
    '　　バイト配列をバイト長で切出し
    '------------------------------------------------------------------------
    '　 [引数]
    '		myByte			= 文字列
    '		strStart		= 開始位置
    '		strLength		= バイト長
    '	[戻り値]
    '		String			= 文字列
    '------------------------------------------------------------------------
    Public Function MidB2B(ByRef myByte As Byte(), _
           ByVal bSt As Long, _
           ByVal bLen As Long) As Byte()
        Dim cBt As Byte()
        ReDim cBt(bLen - 1)
        ReDim MidB2B(bLen - 1)

        '文字列バイト任意切り出し
        Dim i, j As Integer
        j = 0
        i = 0
        For i = bSt - 1 To bSt - 1 + bLen - 1
            cBt(j) = myByte(i)
            j = j + 1
        Next
        MidB2B = cBt
    End Function

    '------------------------------------------------------------------------
    '　　文字列をバイト配列に変換
    '------------------------------------------------------------------------
    '　 [引数]
    '		myString		= 文字列
    '	[戻り値]
    '		Byte()			= バイト配列
    '------------------------------------------------------------------------
    Public Function Str2Byte(ByRef myString As String) As Byte()
        'Shift JISに変換する
        Str2Byte = System.Text.Encoding.GetEncoding(932).GetBytes(myString)
    End Function


    '''''''''''''''''''' 共通構造体 ''''''''''''''''''''''''''''''''''''''''

    '<年月日>
    Public Structure YMD
        Public Year As String     ''年
        Public Month As String     ''月
        Public Day As String     ''日
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Year = MidB2S(bBuff, 1, 4)
            Month = MidB2S(bBuff, 5, 2)
            Day = MidB2S(bBuff, 7, 2)
        End Sub
    End Structure

    '<時分秒>
    Public Structure HMS
        Public Hour As String     ''時
        Public Minute As String     ''分
        Public Second As String     ''秒
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Hour = MidB2S(bBuff, 1, 2)
            Minute = MidB2S(bBuff, 3, 2)
            Second = MidB2S(bBuff, 5, 2)
        End Sub
    End Structure

    '<時分>
    Public Structure HM
        Public Hour As String     ''時
        Public Minute As String     ''分
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Hour = MidB2S(bBuff, 1, 2)
            Minute = MidB2S(bBuff, 3, 2)
        End Sub
    End Structure

    '<月日時分>
    Public Structure MDHM
        Public Month As String     ''月
        Public Day As String     ''日
        Public Hour As String     ''時
        Public Minute As String     ''分
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Month = MidB2S(bBuff, 1, 2)
            Day = MidB2S(bBuff, 3, 2)
            Hour = MidB2S(bBuff, 5, 2)
            Minute = MidB2S(bBuff, 7, 2)
        End Sub
    End Structure

    '<レコードヘッダ>
    Public Structure RECORD_ID
        Public RecordSpec As String    ''レコード種別
        Public DataKubun As String    ''データ区分
        Public MakeDate As YMD     ''データ作成年月日
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            RecordSpec = MidB2S(bBuff, 1, 2)
            DataKubun = MidB2S(bBuff, 3, 1)
            MakeDate.SetDataB(MidB2B(bBuff, 4, 8))
        End Sub
    End Structure

    '<競走識別情報>
    Public Structure RACE_ID
        Public Year As String     ''開催年
        Public MonthDay As String    ''開催月日
        Public JyoCD As String     ''競馬場コード
        Public Kaiji As String     ''開催回[第N回]
        Public Nichiji As String    ''開催日目[N日目]
        Public RaceNum As String    ''レース番号
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Year = MidB2S(bBuff, 1, 4)
            MonthDay = MidB2S(bBuff, 5, 4)
            JyoCD = MidB2S(bBuff, 9, 2)
            Kaiji = MidB2S(bBuff, 11, 2)
            Nichiji = MidB2S(bBuff, 13, 2)
            RaceNum = MidB2S(bBuff, 15, 2)
        End Sub
    End Structure

    '<競走識別情報２>
    Public Structure RACE_ID2
        Public Year As String     ''開催年
        Public MonthDay As String    ''開催月日
        Public JyoCD As String     ''競馬場コード
        Public Kaiji As String     ''開催回[第N回]
        Public Nichiji As String    ''開催日目[N日目]
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Year = MidB2S(bBuff, 1, 4)
            MonthDay = MidB2S(bBuff, 5, 4)
            JyoCD = MidB2S(bBuff, 9, 2)
            Kaiji = MidB2S(bBuff, 11, 2)
            Nichiji = MidB2S(bBuff, 13, 2)
        End Sub
    End Structure

    '<本年・累計成績情報>
    Public Structure SEI_RUIKEI_INFO
        Public SetYear As String    ''設定年
        Public HonSyokinTotal As String   ''本賞金合計
        Public FukaSyokin As String    ''付加賞金合計
        Public ChakuKaisu() As String   ''着回数
        '配列の初期化
        Public Sub Initialize()
            ReDim ChakuKaisu(5)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            SetYear = MidB2S(bBuff, 1, 4)
            HonSyokinTotal = MidB2S(bBuff, 5, 10)
            FukaSyokin = MidB2S(bBuff, 15, 10)
            Dim i As Integer = 0
            For i = 0 To 5
                ChakuKaisu(i) = MidB2S(bBuff, 25 + 6 * i, 6)
            Next i
        End Sub
    End Structure

    '<最近重賞勝利情報>
    Public Structure SAIKIN_JYUSYO_INFO
        Public SaikinJyusyoid As RACE_ID  ''<年月日場回日R>
        Public Hondai As String     ''競走名本題
        Public Ryakusyo10 As String    ''競走名略称10字
        Public Ryakusyo6 As String    ''競走名略称6字
        Public Ryakusyo3 As String    ''競走名略称3字
        Public GradeCD As String    ''グレードコード
        Public SyussoTosu As String    ''出走頭数
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            SaikinJyusyoid.SetDataB(MidB2B(bBuff, 1, 16))
            Hondai = MidB2S(bBuff, 17, 60)
            Ryakusyo10 = MidB2S(bBuff, 77, 20)
            Ryakusyo6 = MidB2S(bBuff, 97, 12)
            Ryakusyo3 = MidB2S(bBuff, 109, 6)
            GradeCD = MidB2S(bBuff, 115, 1)
            SyussoTosu = MidB2S(bBuff, 116, 2)
            KettoNum = MidB2S(bBuff, 118, 10)
            Bamei = MidB2S(bBuff, 128, 36)
        End Sub
    End Structure

    '<本年・前年・累計成績情報>
    Public Structure HON_ZEN_RUIKEISEI_INFO
        Public SetYear As String    ''設定年
        Public HonSyokinHeichi As String  ''平地本賞金合計
        Public HonSyokinSyogai As String  ''障害本賞金合計
        Public FukaSyokinHeichi As String  ''平地付加賞金合計
        Public FukaSyokinSyogai As String  ''障害付加賞金合計
        Public ChakuKaisuHeichi As CHAKUKAISU6_INFO  ''平地着回数
        Public ChakuKaisuSyogai As CHAKUKAISU6_INFO  ''障害着回数
        Public ChakuKaisuJyo() As CHAKUKAISU6_INFO  ''競馬場別着回数
        Public ChakuKaisuKyori() As CHAKUKAISU6_INFO ''距離別着回数
        '配列の初期化
        Public Sub Initialize()
            ReDim ChakuKaisuJyo(19)
            ReDim ChakuKaisuKyori(5)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            SetYear = MidB2S(bBuff, 1, 4)
            HonSyokinHeichi = MidB2S(bBuff, 5, 10)
            HonSyokinSyogai = MidB2S(bBuff, 15, 10)
            FukaSyokinHeichi = MidB2S(bBuff, 25, 10)
            FukaSyokinSyogai = MidB2S(bBuff, 35, 10)
            ChakuKaisuHeichi.SetDataB(MidB2B(bBuff, 45, 36))
            ChakuKaisuSyogai.SetDataB(MidB2B(bBuff, 81, 36))
            Dim i As Integer = 0
            For i = 0 To 19
                ChakuKaisuJyo(i).SetDataB(MidB2B(bBuff, 117 + 36 * i, 36))
            Next i
            For i = 0 To 5
                ChakuKaisuKyori(i).SetDataB(MidB2B(bBuff, 837 + 36 * i, 36))
            Next i
        End Sub
    End Structure

    '<レース情報>
    Public Structure RACE_INFO
        Public YoubiCD As String    ''曜日コード
        Public TokuNum As String    ''特別競走番号
        Public Hondai As String     ''競走名本題
        Public Fukudai As String    ''競走名副題
        Public Kakko As String     ''競走名カッコ内
        Public HondaiEng As String    ''競走名本題欧字
        Public FukudaiEng As String    ''競走名副題欧字
        Public KakkoEng As String    ''競走名カッコ内欧字
        Public Ryakusyo10 As String    ''競走名略称１０字
        Public Ryakusyo6 As String    ''競走名略称６字
        Public Ryakusyo3 As String    ''競走名略称３字
        Public Kubun As String     ''競走名区分
        Public Nkai As String     ''重賞回次[第N回]
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            YoubiCD = MidB2S(bBuff, 1, 1)
            TokuNum = MidB2S(bBuff, 2, 4)
            Hondai = MidB2S(bBuff, 6, 60)
            Fukudai = MidB2S(bBuff, 66, 60)
            Kakko = MidB2S(bBuff, 126, 60)
            HondaiEng = MidB2S(bBuff, 186, 120)
            FukudaiEng = MidB2S(bBuff, 306, 120)
            KakkoEng = MidB2S(bBuff, 426, 120)
            Ryakusyo10 = MidB2S(bBuff, 546, 20)
            Ryakusyo6 = MidB2S(bBuff, 566, 12)
            Ryakusyo3 = MidB2S(bBuff, 578, 6)
            Kubun = MidB2S(bBuff, 584, 1)
            Nkai = MidB2S(bBuff, 585, 3)
        End Sub
    End Structure

    '<天候・馬場状態>
    Public Structure TENKO_BABA_INFO
        Public TenkoCD As String    ''天候コード
        Public SibaBabaCD As String    ''芝馬場状態コード
        Public DirtBabaCD As String    ''ダート馬場状態コード
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            TenkoCD = MidB2S(bBuff, 1, 1)
            SibaBabaCD = MidB2S(bBuff, 2, 1)
            DirtBabaCD = MidB2S(bBuff, 3, 1)
        End Sub
    End Structure

    '<着回数（サイズ3byte）>
    Public Structure CHAKUKAISU3_INFO
        Public Chakukaisu() As String
        '配列の初期化
        Public Sub Initialize()
            ReDim Chakukaisu(5)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            Dim i As Integer = 0
            For i = 0 To 5
                Chakukaisu(i) = MidB2S(bBuff, 1 + 3 * i, 3)
            Next i
        End Sub
    End Structure

    '<着回数（サイズ4byte）>
    Public Structure CHAKUKAISU4_INFO
        Public Chakukaisu() As String
        '配列の初期化
        Public Sub Initialize()
            ReDim Chakukaisu(5)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            Dim i As Integer = 0
            For i = 0 To 5
                Chakukaisu(i) = MidB2S(bBuff, 1 + 4 * i, 4)
            Next i
        End Sub
    End Structure

    '<着回数（サイズ5byte）>
    Public Structure CHAKUKAISU5_INFO
        Public Chakukaisu() As String
        '配列の初期化
        Public Sub Initialize()
            ReDim Chakukaisu(5)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            Dim i As Integer = 0
            For i = 0 To 5
                Chakukaisu(i) = MidB2S(bBuff, 1 + 5 * i, 5)
            Next i
        End Sub
    End Structure

    '<着回数（サイズ6byte）>
    Public Structure CHAKUKAISU6_INFO
        Public Chakukaisu() As String
        '配列の初期化
        Public Sub Initialize()
            ReDim Chakukaisu(5)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            Dim i As Integer = 0
            For i = 0 To 5
                Chakukaisu(i) = MidB2S(bBuff, 1 + (6 * i), 6)
            Next i
        End Sub
    End Structure

    '<競走条件コード>
    Public Structure RACE_JYOKEN
        Public SyubetuCD As String      ''競走種別コード
        Public KigoCD As String       ''競走記号コード
        Public JyuryoCD As String      ''重量種別コード
        Public JyokenCD() As String      ''競走条件コード
        '配列の初期化
        Public Sub Initialize()
            ReDim JyokenCD(4)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            SyubetuCD = MidB2S(bBuff, 1, 2)
            KigoCD = MidB2S(bBuff, 3, 3)
            JyuryoCD = MidB2S(bBuff, 6, 1)
            Dim i As Integer = 0
            For i = 0 To 4
                JyokenCD(i) = MidB2S(bBuff, 7 + 3 * i, 3)
            Next i
        End Sub
    End Structure

    '''''''''''''''''''' データ構造体 ''''''''''''''''''''''''''''''

    '****** １．特別登録馬 ****************************************
    '<登録馬毎情報>
    Public Structure TOKUUMA_INFO
        Public Num As String     ''連番
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        Public UmaKigoCD As String    ''馬記号コード
        Public SexCD As String     ''性別コード
        Public TozaiCD As String    ''調教師東西所属コード
        Public ChokyosiCode As String   ''調教師コード
        Public ChokyosiRyakusyo As String  ''調教師名略称
        Public Futan As String     ''負担重量
        Public Koryu As String     ''交流区分
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Num = MidB2S(bBuff, 1, 3)
            KettoNum = MidB2S(bBuff, 4, 10)
            Bamei = MidB2S(bBuff, 14, 36)
            UmaKigoCD = MidB2S(bBuff, 50, 2)
            SexCD = MidB2S(bBuff, 52, 1)
            TozaiCD = MidB2S(bBuff, 53, 1)
            ChokyosiCode = MidB2S(bBuff, 54, 5)
            ChokyosiRyakusyo = MidB2S(bBuff, 59, 8)
            Futan = MidB2S(bBuff, 67, 3)
            Koryu = MidB2S(bBuff, 70, 1)
        End Sub
    End Structure
    Public Structure JV_TK_TOKUUMA
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public RaceInfo As RACE_INFO   ''<レース情報>
        Public GradeCD As String    ''グレードコード
        Public JyokenInfo As RACE_JYOKEN  ''<競走条件コード>
        Public Kyori As String     ''距離
        Public TrackCD As String    ''トラックコード
        Public CourseKubunCD As String   ''コース区分
        Public HandiDate As YMD     ''ハンデ発表日
        Public TorokuTosu As String    ''登録頭数
        Public TokuUmaInfo() As TOKUUMA_INFO ''<登録馬毎情報>
        Public crlf As String     ''レコード区切
        '配列の初期化
        Public Sub Initialize()
            ReDim TokuUmaInfo(299)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 21657
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            RaceInfo.SetDataB(MidB2B(bBuff, 28, 587))
            GradeCD = MidB2S(bBuff, 615, 1)
            JyokenInfo.SetDataB(MidB2B(bBuff, 616, 21))
            Kyori = MidB2S(bBuff, 637, 4)
            TrackCD = MidB2S(bBuff, 641, 2)
            CourseKubunCD = MidB2S(bBuff, 643, 2)
            HandiDate.SetDataB(MidB2B(bBuff, 645, 8))
            TorokuTosu = MidB2S(bBuff, 653, 3)
            Dim i As Integer
            For i = 0 To 299
                TokuUmaInfo(i).SetDataB(MidB2B(bBuff, 656 + 70 * i, 70))

            Next i
            crlf = MidB2S(bBuff, 21656, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２．レース詳細 ****************************************
    '<コーナー通過順位>
    Public Structure CORNER_INFO
        Public Corner As String     ''コーナー
        Public Syukaisu As String    ''周回数
        Public Jyuni As String     ''各通過順位
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Corner = MidB2S(bBuff, 1, 1)
            Syukaisu = MidB2S(bBuff, 2, 1)
            Jyuni = MidB2S(bBuff, 3, 70)
        End Sub
    End Structure
    Public Structure JV_RA_RACE
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public RaceInfo As RACE_INFO   ''<レース情報>
        Public GradeCD As String    ''グレードコード
        Public GradeCDBefore As String   ''変更前グレードコード
        Public JyokenInfo As RACE_JYOKEN  ''<競走条件コード>
        Public JyokenName As String    ''競走条件名称
        Public Kyori As String     ''距離
        Public KyoriBefore As String   ''変更前距離
        Public TrackCD As String    ''トラックコード
        Public TrackCDBefore As String   ''変更前トラックコード
        Public CourseKubunCD As String   ''コース区分
        Public CourseKubunCDBefore As String ''変更前コース区分
        Public Honsyokin() As String   ''本賞金
        Public HonsyokinBefore() As String  ''変更前本賞金
        Public Fukasyokin() As String   ''付加賞金
        Public FukasyokinBefore() As String  ''変更前付加賞金
        Public HassoTime As String    ''発走時刻
        Public HassoTimeBefore As String  ''変更前発走時刻
        Public TorokuTosu As String    ''登録頭数
        Public SyussoTosu As String    ''出走頭数
        Public NyusenTosu As String    ''入線頭数
        Public TenkoBaba As TENKO_BABA_INFO  ''天候・馬場状態コード
        Public LapTime() As String    ''ラップタイム
        Public SyogaiMileTime As String   ''障害マイルタイム
        Public HaronTimeS3 As String   ''前３ハロンタイム
        Public HaronTimeS4 As String   ''前４ハロンタイム
        Public HaronTimeL3 As String   ''後３ハロンタイム
        Public HaronTimeL4 As String   ''後４ハロンタイム
        Public CornerInfo() As CORNER_INFO  ''<コーナー通過順位>
        Public RecordUpKubun As String   ''レコード更新区分
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim Honsyokin(6)
            ReDim HonsyokinBefore(4)
            ReDim Fukasyokin(4)
            ReDim FukasyokinBefore(2)
            ReDim LapTime(24)
            ReDim CornerInfo(3)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 1272
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            RaceInfo.SetDataB(MidB2B(bBuff, 28, 587))
            GradeCD = MidB2S(bBuff, 615, 1)
            GradeCDBefore = MidB2S(bBuff, 616, 1)
            JyokenInfo.SetDataB(MidB2B(bBuff, 617, 21))
            JyokenName = MidB2S(bBuff, 638, 60)
            Kyori = MidB2S(bBuff, 698, 4)
            KyoriBefore = MidB2S(bBuff, 702, 4)
            TrackCD = MidB2S(bBuff, 706, 2)
            TrackCDBefore = MidB2S(bBuff, 708, 2)
            CourseKubunCD = MidB2S(bBuff, 710, 2)
            CourseKubunCDBefore = MidB2S(bBuff, 712, 2)
            For i = 0 To 6
                Honsyokin(i) = MidB2S(bBuff, 714 + 8 * i, 8)
            Next i
            For i = 0 To 4
                HonsyokinBefore(i) = MidB2S(bBuff, 770 + 8 * i, 8)
            Next i
            For i = 0 To 4
                Fukasyokin(i) = MidB2S(bBuff, 810 + 8 * i, 8)
            Next i
            For i = 0 To 2
                FukasyokinBefore(i) = MidB2S(bBuff, 850 + 8 * i, 8)
            Next i
            HassoTime = MidB2S(bBuff, 874, 4)
            HassoTimeBefore = MidB2S(bBuff, 878, 4)
            TorokuTosu = MidB2S(bBuff, 882, 2)
            SyussoTosu = MidB2S(bBuff, 884, 2)
            NyusenTosu = MidB2S(bBuff, 886, 2)
            TenkoBaba.SetDataB(MidB2B(bBuff, 888, 3))
            For i = 0 To 24
                LapTime(i) = MidB2S(bBuff, 891 + 3 * i, 3)
            Next i
            SyogaiMileTime = MidB2S(bBuff, 966, 4)
            HaronTimeS3 = MidB2S(bBuff, 970, 3)
            HaronTimeS4 = MidB2S(bBuff, 973, 3)
            HaronTimeL3 = MidB2S(bBuff, 976, 3)
            HaronTimeL4 = MidB2S(bBuff, 979, 3)
            For i = 0 To 3
                CornerInfo(i).SetDataB(MidB2B(bBuff, 982 + 72 * i, 72))
            Next i
            RecordUpKubun = MidB2S(bBuff, 1270, 1)
            crlf = MidB2S(bBuff, 1271, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ３．馬毎レース情報 ****************************************
    '<1着馬(相手馬)情報>
    Public Structure CHAKUUMA_INFO
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            KettoNum = MidB2S(bBuff, 1, 10)
            Bamei = MidB2S(bBuff, 11, 36)
        End Sub
    End Structure
    Public Structure JV_SE_RACE_UMA
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public Wakuban As String       ''枠番
        Public Umaban As String     ''馬番
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        Public UmaKigoCD As String    ''馬記号コード
        Public SexCD As String     ''性別コード
        Public HinsyuCD As String    ''品種コード
        Public KeiroCD As String    ''毛色コード
        Public Barei As String     ''馬齢
        Public TozaiCD As String    ''東西所属コード
        Public ChokyosiCode As String   ''調教師コード
        Public ChokyosiRyakusyo As String  ''調教師名略称
        Public BanusiCode As String    ''馬主コード
        Public BanusiName As String    ''馬主名
        Public Fukusyoku As String    ''服色標示
        Public reserved1 As String    ''予備
        Public Futan As String     ''負担重量
        Public FutanBefore As String   ''変更前負担重量
        Public Blinker As String    ''ブリンカー使用区分
        Public reserved2 As String    ''予備
        Public KisyuCode As String    ''騎手コード
        Public KisyuCodeBefore As String  ''変更前騎手コード
        Public KisyuRyakusyo As String   ''騎手名略称
        Public KisyuRyakusyoBefore As String ''変更前騎手名略称
        Public MinaraiCD As String    ''騎手見習コード
        Public MinaraiCDBefore As String  ''変更前騎手見習コード
        Public BaTaijyu As String    ''馬体重
        Public ZogenFugo As String    ''増減符号
        Public ZogenSa As String    ''増減差
        Public IJyoCD As String     ''異常区分コード
        Public NyusenJyuni As String   ''入線順位
        Public KakuteiJyuni As String   ''確定着順
        Public DochakuKubun As String   ''同着区分
        Public DochakuTosu As String   ''同着頭数
        Public Time As String     ''走破タイム
        Public ChakusaCD As String    ''着差コード
        Public ChakusaCDP As String    ''+着差コード
        Public ChakusaCDPP As String   ''++着差コード
        Public Jyuni1c As String    ''1コーナーでの順位
        Public Jyuni2c As String    ''2コーナーでの順位
        Public Jyuni3c As String    ''3コーナーでの順位
        Public Jyuni4c As String    ''4コーナーでの順位
        Public Odds As String     ''単勝オッズ
        Public Ninki As String     ''単勝人気順
        Public Honsyokin As String    ''獲得本賞金
        Public Fukasyokin As String    ''獲得付加賞金
        Public reserved3 As String    ''予備
        Public reserved4 As String    ''予備
        Public HaronTimeL4 As String   ''後４ハロンタイム
        Public HaronTimeL3 As String   ''後３ハロンタイム
        Public ChakuUmaInfo() As CHAKUUMA_INFO ''<1着馬(相手馬)情報>
        Public TimeDiff As String    ''タイム差
        Public RecordUpKubun As String   ''レコード更新区分
        Public DMKubun As String    ''マイニング区分
        Public DMTime As String     ''マイニング予想走破タイム
        Public DMGosaP As String    ''予測誤差(信頼度)＋
        Public DMGosaM As String    ''予測誤差(信頼度)－
        Public DMJyuni As String    ''マイニング予想順位
        Public KyakusituKubun As String   ''今回レース脚質判定
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim ChakuUmaInfo(2)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 555
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            Wakuban = MidB2S(bBuff, 28, 1)
            Umaban = MidB2S(bBuff, 29, 2)
            KettoNum = MidB2S(bBuff, 31, 10)
            Bamei = MidB2S(bBuff, 41, 36)
            UmaKigoCD = MidB2S(bBuff, 77, 2)
            SexCD = MidB2S(bBuff, 79, 1)
            HinsyuCD = MidB2S(bBuff, 80, 1)
            KeiroCD = MidB2S(bBuff, 81, 2)
            Barei = MidB2S(bBuff, 83, 2)
            TozaiCD = MidB2S(bBuff, 85, 1)
            ChokyosiCode = MidB2S(bBuff, 86, 5)
            ChokyosiRyakusyo = MidB2S(bBuff, 91, 8)
            BanusiCode = MidB2S(bBuff, 99, 6)
            BanusiName = MidB2S(bBuff, 105, 64)
            Fukusyoku = MidB2S(bBuff, 169, 60)
            reserved1 = MidB2S(bBuff, 229, 60)
            Futan = MidB2S(bBuff, 289, 3)
            FutanBefore = MidB2S(bBuff, 292, 3)
            Blinker = MidB2S(bBuff, 295, 1)
            reserved2 = MidB2S(bBuff, 296, 1)
            KisyuCode = MidB2S(bBuff, 297, 5)
            KisyuCodeBefore = MidB2S(bBuff, 302, 5)
            KisyuRyakusyo = MidB2S(bBuff, 307, 8)
            KisyuRyakusyoBefore = MidB2S(bBuff, 315, 8)
            MinaraiCD = MidB2S(bBuff, 323, 1)
            MinaraiCDBefore = MidB2S(bBuff, 324, 1)
            BaTaijyu = MidB2S(bBuff, 325, 3)
            ZogenFugo = MidB2S(bBuff, 328, 1)
            ZogenSa = MidB2S(bBuff, 329, 3)
            IJyoCD = MidB2S(bBuff, 332, 1)
            NyusenJyuni = MidB2S(bBuff, 333, 2)
            KakuteiJyuni = MidB2S(bBuff, 335, 2)
            DochakuKubun = MidB2S(bBuff, 337, 1)
            DochakuTosu = MidB2S(bBuff, 338, 1)
            Time = MidB2S(bBuff, 339, 4)
            ChakusaCD = MidB2S(bBuff, 343, 3)
            ChakusaCDP = MidB2S(bBuff, 346, 3)
            ChakusaCDPP = MidB2S(bBuff, 349, 3)
            Jyuni1c = MidB2S(bBuff, 352, 2)
            Jyuni2c = MidB2S(bBuff, 354, 2)
            Jyuni3c = MidB2S(bBuff, 356, 2)
            Jyuni4c = MidB2S(bBuff, 358, 2)
            Odds = MidB2S(bBuff, 360, 4)
            Ninki = MidB2S(bBuff, 364, 2)
            Honsyokin = MidB2S(bBuff, 366, 8)
            Fukasyokin = MidB2S(bBuff, 374, 8)
            reserved3 = MidB2S(bBuff, 382, 3)
            reserved4 = MidB2S(bBuff, 385, 3)
            HaronTimeL4 = MidB2S(bBuff, 388, 3)
            HaronTimeL3 = MidB2S(bBuff, 391, 3)
            For i = 0 To 2
                ChakuUmaInfo(i).SetDataB(MidB2B(bBuff, 394 + 46 * i, 46))
            Next i
            TimeDiff = MidB2S(bBuff, 532, 4)
            RecordUpKubun = MidB2S(bBuff, 536, 1)
            DMKubun = MidB2S(bBuff, 537, 1)
            DMTime = MidB2S(bBuff, 538, 5)
            DMGosaP = MidB2S(bBuff, 543, 4)
            DMGosaM = MidB2S(bBuff, 547, 4)
            DMJyuni = MidB2S(bBuff, 551, 2)
            KyakusituKubun = MidB2S(bBuff, 553, 1)
            crlf = MidB2S(bBuff, 554, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ４．払戻 ****************************************

    ''<払戻情報１ 単・複・枠>
    Public Structure PAY_INFO1
        Public Umaban As String     ''馬番
        Public Pay As String     ''払戻金
        Public Ninki As String     ''人気順	
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            Pay = MidB2S(bBuff, 3, 9)
            Ninki = MidB2S(bBuff, 12, 2)
        End Sub
    End Structure

    ''<払戻情報２ 馬連・ワイド・予備・馬単>
    Public Structure PAY_INFO2
        Public Kumi As String     ''組番
        Public Pay As String     ''払戻金
        Public Ninki As String     ''人気順	
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 4)
            Pay = MidB2S(bBuff, 5, 9)
            Ninki = MidB2S(bBuff, 14, 3)
        End Sub
    End Structure

    ''<払戻情報３ ３連複>
    Public Structure PAY_INFO3
        Public Kumi As String     ''組番
        Public Pay As String     ''払戻金
        Public Ninki As String     ''人気順	
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 6)
            Pay = MidB2S(bBuff, 7, 9)
            Ninki = MidB2S(bBuff, 16, 3)
        End Sub
    End Structure

    ''<払戻情報４ ３連単>
    Public Structure PAY_INFO4
        Public Kumi As String     ''組番
        Public Pay As String     ''払戻金
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 6)
            Pay = MidB2S(bBuff, 7, 9)
            Ninki = MidB2S(bBuff, 16, 4)
        End Sub
    End Structure

    Public Structure JV_HR_PAY
        Public head As RECORD_ID            ''<レコードヘッダー>
        Public id As RACE_ID                ''<競走識別情報>
        Public TorokuTosu As String         ''登録頭数
        Public SyussoTosu As String         ''出走頭数
        Public FuseirituFlag() As String    ''不成立フラグ
        Public TokubaraiFlag() As String    ''特払フラグ
        Public HenkanFlag() As String       ''返還フラグ
        Public HenkanUma() As String        ''返還馬番情報(馬番01～28)
        Public HenkanWaku() As String       ''返還枠番情報(枠番1～8)
        Public HenkanDoWaku() As String     ''返還同枠情報(枠番1～8)
        Public PayTansyo() As PAY_INFO1     ''<単勝払戻>
        Public PayFukusyo() As PAY_INFO1    ''<複勝払戻>
        Public PayWakuren() As PAY_INFO1    ''<枠連払戻>
        Public PayUmaren() As PAY_INFO2     ''<馬連払戻>
        Public PayWide() As PAY_INFO2       ''<ワイド払戻>
        Public PayReserved1() As PAY_INFO2  ''<予備>
        Public PayUmatan() As PAY_INFO2     ''<馬単払戻>
        Public PaySanrenpuku() As PAY_INFO3 ''<3連複払戻>
        Public PaySanrentan() As PAY_INFO4  ''<3連単払戻>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim FuseirituFlag(8)
            ReDim TokubaraiFlag(8)
            ReDim HenkanFlag(8)
            ReDim HenkanUma(27)
            ReDim HenkanWaku(7)
            ReDim HenkanDoWaku(7)
            ReDim PayTansyo(2)
            ReDim PayFukusyo(4)
            ReDim PayWakuren(2)
            ReDim PayUmaren(2)
            ReDim PayWide(6)
            ReDim PayReserved1(2)
            ReDim PayUmatan(5)
            ReDim PaySanrenpuku(2)
            ReDim PaySanrentan(5)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 719
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            TorokuTosu = MidB2S(bBuff, 28, 2)
            SyussoTosu = MidB2S(bBuff, 30, 2)
            For i = 0 To 8
                FuseirituFlag(i) = MidB2S(bBuff, 32 + (1 * i), 1)
            Next i
            For i = 0 To 8
                TokubaraiFlag(i) = MidB2S(bBuff, 41 + (1 * i), 1)
            Next i
            For i = 0 To 8
                HenkanFlag(i) = MidB2S(bBuff, 50 + (1 * i), 1)
            Next i
            For i = 0 To 27
                HenkanUma(i) = MidB2S(bBuff, 59 + (1 * i), 1)
            Next i
            For i = 0 To 7
                HenkanWaku(i) = MidB2S(bBuff, 87 + (1 * i), 1)
            Next i
            For i = 0 To 7
                HenkanDoWaku(i) = MidB2S(bBuff, 95 + (1 * i), 1)
            Next i
            For i = 0 To 2
                PayTansyo(i).SetDataB(MidB2B(bBuff, 103 + (13 * i), 13))
            Next i
            For i = 0 To 4
                PayFukusyo(i).SetDataB(MidB2B(bBuff, 142 + (13 * i), 13))
            Next i
            For i = 0 To 2
                PayWakuren(i).SetDataB(MidB2B(bBuff, 207 + (13 * i), 13))
            Next i
            For i = 0 To 2
                PayUmaren(i).SetDataB(MidB2B(bBuff, 246 + (16 * i), 16))
            Next i
            For i = 0 To 6
                PayWide(i).SetDataB(MidB2B(bBuff, 294 + (16 * i), 16))
            Next i
            For i = 0 To 2
                PayReserved1(i).SetDataB(MidB2B(bBuff, 406 + (16 * i), 16))
            Next i
            For i = 0 To 5
                PayUmatan(i).SetDataB(MidB2B(bBuff, 454 + (16 * i), 16))
            Next i
            For i = 0 To 2
                PaySanrenpuku(i).SetDataB(MidB2B(bBuff, 550 + (18 * i), 18))
            Next i
            For i = 0 To 5
                PaySanrentan(i).SetDataB(MidB2B(bBuff, 604 + (19 * i), 19))
            Next i
            crlf = MidB2S(bBuff, 718, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ５．票数（全掛式）****************************************
    '<票数情報１ 単・複・枠>
    Public Structure HYO_INFO1
        Public Umaban As String     ''馬番		
        Public Hyo As String     ''票数
        Public Ninki As String     ''人気
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            Hyo = MidB2S(bBuff, 3, 11)
            Ninki = MidB2S(bBuff, 14, 2)
        End Sub
    End Structure
    '<票数情報２ 馬連・ワイド・馬単>
    Public Structure HYO_INFO2
        Public Kumi As String     ''組番		
        Public Hyo As String     ''票数
        Public Ninki As String     ''人気
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 4)
            Hyo = MidB2S(bBuff, 5, 11)
            Ninki = MidB2S(bBuff, 16, 3)
        End Sub
    End Structure
    '<票数情報３ ３連複票数>
    Public Structure HYO_INFO3
        Public Kumi As String     ''組番		
        Public Hyo As String     ''票数
        Public Ninki As String     ''人気
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 6)
            Hyo = MidB2S(bBuff, 7, 11)
            Ninki = MidB2S(bBuff, 18, 3)
        End Sub
    End Structure
    '<票数情報４ ３連単票数>
    Public Structure HYO_INFO4
        Public Kumi As String     ''組番		
        Public Hyo As String     ''票数
        Public Ninki As String     ''人気
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 6)
            Hyo = MidB2S(bBuff, 7, 11)
            Ninki = MidB2S(bBuff, 18, 4)
        End Sub
    End Structure

    Public Structure JV_H1_HYOSU_ZENKAKE
        Public head As RECORD_ID            ''<レコードヘッダー>
        Public id As RACE_ID                ''<競走識別情報>
        Public TorokuTosu As String         ''登録頭数
        Public SyussoTosu As String         ''出走頭数
        Public HatubaiFlag() As String      ''発売フラグ　
        Public FukuChakuBaraiKey As String  ''複勝着払キー
        Public HenkanUma() As String        ''返還馬番情報(馬番01～28)
        Public HenkanWaku() As String       ''返還枠番情報(枠番1～8)
        Public HenkanDoWaku() As String     ''返還同枠情報(枠番1～8)
        Public HyoTansyo() As HYO_INFO1     ''<単勝票数>
        Public HyoFukusyo() As HYO_INFO1    ''<複勝票数>
        Public HyoWakuren() As HYO_INFO1    ''<枠連票数>
        Public HyoUmaren() As HYO_INFO2     ''<馬連票数>
        Public HyoWide() As HYO_INFO2       ''<ワイド票数>
        Public HyoUmatan() As HYO_INFO2     ''<馬単票数>
        Public HyoSanrenpuku() As HYO_INFO3 ''<3連複票数>
        Public HyoTotal() As String         ''票数合計
        Public crlf As String               ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim HatubaiFlag(6)
            ReDim HenkanUma(27)
            ReDim HenkanWaku(7)
            ReDim HenkanDoWaku(7)
            ReDim HyoTansyo(27)
            ReDim HyoFukusyo(27)
            ReDim HyoWakuren(35)
            ReDim HyoUmaren(152)
            ReDim HyoWide(152)
            ReDim HyoUmatan(305)
            ReDim HyoSanrenpuku(815)
            ReDim HyoTotal(13)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 28955
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            TorokuTosu = MidB2S(bBuff, 28, 2)
            SyussoTosu = MidB2S(bBuff, 30, 2)
            For i = 0 To 6
                HatubaiFlag(i) = MidB2S(bBuff, 32 + (1 * i), 1)
            Next i
            FukuChakuBaraiKey = MidB2S(bBuff, 39, 1)
            For i = 0 To 27
                HenkanUma(i) = MidB2S(bBuff, 40 + (1 * i), 1)
            Next i
            For i = 0 To 7
                HenkanWaku(i) = MidB2S(bBuff, 68 + (1 * i), 1)
            Next i
            For i = 0 To 7
                HenkanDoWaku(i) = MidB2S(bBuff, 76 + (1 * i), 1)
            Next i
            For i = 0 To 27
                HyoTansyo(i).SetDataB(MidB2B(bBuff, 84 + (15 * i), 15))
            Next i
            For i = 0 To 27
                HyoFukusyo(i).SetDataB(MidB2B(bBuff, 504 + (15 * i), 15))
            Next i
            For i = 0 To 35
                HyoWakuren(i).SetDataB(MidB2B(bBuff, 924 + (15 * i), 15))
            Next i
            For i = 0 To 152
                HyoUmaren(i).SetDataB(MidB2B(bBuff, 1464 + (18 * i), 18))
            Next i
            For i = 0 To 152
                HyoWide(i).SetDataB(MidB2B(bBuff, 4218 + (18 * i), 18))
            Next i
            For i = 0 To 305
                HyoUmatan(i).SetDataB(MidB2B(bBuff, 6972 + (18 * i), 18))
            Next i
            For i = 0 To 815
                HyoSanrenpuku(i).SetDataB(MidB2B(bBuff, 12480 + (20 * i), 20))
            Next i
            For i = 0 To 13
                HyoTotal(i) = MidB2S(bBuff, 28800 + (11 * i), 11)
            Next i
            crlf = MidB2S(bBuff, 28954, 2)
            bBuff = Nothing
        End Sub
    End Structure

    Public Structure JV_H6_HYOSU_SANRENTAN
        Public head As RECORD_ID            ''<レコードヘッダー>
        Public id As RACE_ID                ''<競走識別情報>
        Public TorokuTosu As String         ''登録頭数
        Public SyussoTosu As String         ''出走頭数
        Public HatubaiFlag As String        ''発売フラグ　
        Public HenkanUma() As String        ''返還馬番情報(馬番01～18)
        Public HyoSanrentan() As HYO_INFO4 ''<3連単票数>
        Public HyoTotal() As String         ''票数合計
        Public crlf As String               ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim HenkanUma(17)
            ReDim HyoSanrentan(4895)
            ReDim HyoTotal(1)
        End Sub

        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 102900
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            TorokuTosu = MidB2S(bBuff, 28, 2)
            SyussoTosu = MidB2S(bBuff, 30, 2)
            HatubaiFlag = MidB2S(bBuff, 32, 1)
            For i = 0 To 17
                HenkanUma(i) = MidB2S(bBuff, 33 + (1 * i), 1)
            Next i
            For i = 0 To 4895
                HyoSanrentan(i).SetDataB(MidB2B(bBuff, 51 + (21 * i), 21))
            Next i
            For i = 0 To 1
                HyoTotal(i) = MidB2S(bBuff, 102867 + (11 * i), 11)
            Next i
            crlf = MidB2S(bBuff, 102889, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ６．オッズ（単複枠）****************************************
    '<単勝オッズ>
    Public Structure ODDS_TANSYO_INFO
        Public Umaban As String     ''馬番
        Public Odds As String     ''オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            Odds = MidB2S(bBuff, 3, 4)
            Ninki = MidB2S(bBuff, 7, 2)
        End Sub
    End Structure
    '<複勝オッズ>
    Public Structure ODDS_FUKUSYO_INFO
        Public Umaban As String     ''馬番
        Public OddsLow As String    ''最低オッズ
        Public OddsHigh As String    ''最高オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            OddsLow = MidB2S(bBuff, 3, 4)
            OddsHigh = MidB2S(bBuff, 7, 4)
            Ninki = MidB2S(bBuff, 11, 2)
        End Sub
    End Structure
    '<枠連オッズ>
    Public Structure ODDS_WAKUREN_INFO
        Public Kumi As String     ''組
        Public Odds As String     ''オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 2)
            Odds = MidB2S(bBuff, 3, 5)
            Ninki = MidB2S(bBuff, 8, 2)
        End Sub
    End Structure
    Public Structure JV_O1_ODDS_TANFUKUWAKU
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public TorokuTosu As String    ''登録頭数
        Public SyussoTosu As String    ''出走頭数
        Public TansyoFlag As String    ''発売フラグ　単勝
        Public FukusyoFlag As String   ''発売フラグ　複勝
        Public WakurenFlag As String   ''発売フラグ　枠連
        Public FukuChakuBaraiKey As String  ''複勝着払キー
        Public OddsTansyoInfo() As ODDS_TANSYO_INFO  ''<単勝オッズ>
        Public OddsFukusyoInfo() As ODDS_FUKUSYO_INFO ''<複勝票数オッズ>
        Public OddsWakurenInfo() As ODDS_WAKUREN_INFO ''<枠連票数オッズ>
        Public TotalHyosuTansyo As String  ''単勝票数合計
        Public TotalHyosuFukusyo As String  ''複勝票数合計
        Public TotalHyosuWakuren As String  ''枠連票数合計
        Public crlf As String   ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim OddsTansyoInfo(27)
            ReDim OddsFukusyoInfo(27)
            ReDim OddsWakurenInfo(35)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 962
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TorokuTosu = MidB2S(bBuff, 36, 2)
            SyussoTosu = MidB2S(bBuff, 38, 2)
            TansyoFlag = MidB2S(bBuff, 40, 1)
            FukusyoFlag = MidB2S(bBuff, 41, 1)
            WakurenFlag = MidB2S(bBuff, 42, 1)
            FukuChakuBaraiKey = MidB2S(bBuff, 43, 1)
            For i = 0 To 27
                OddsTansyoInfo(i).SetDataB(MidB2B(bBuff, 44 + (8 * i), 8))
            Next i
            For i = 0 To 27
                OddsFukusyoInfo(i).SetDataB(MidB2B(bBuff, 268 + (12 * i), 12))
            Next i
            For i = 0 To 35
                OddsWakurenInfo(i).SetDataB(MidB2B(bBuff, 604 + (9 * i), 9))
            Next i
            TotalHyosuTansyo = MidB2S(bBuff, 928, 11)
            TotalHyosuFukusyo = MidB2S(bBuff, 939, 11)
            TotalHyosuWakuren = MidB2S(bBuff, 950, 11)
            crlf = MidB2S(bBuff, 961, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ７．オッズ（馬連）****************************************
    '<馬連オッズ>
    Public Structure ODDS_UMAREN_INFO
        Public Kumi As String     ''組番
        Public Odds As String     ''オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 4)
            Odds = MidB2S(bBuff, 5, 6)
            Ninki = MidB2S(bBuff, 11, 3)
        End Sub
    End Structure
    Public Structure JV_O2_ODDS_UMAREN
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public TorokuTosu As String    ''登録頭数
        Public SyussoTosu As String    ''出走頭数
        Public UmarenFlag As String    ''発売フラグ　馬連
        Public OddsUmarenInfo() As ODDS_UMAREN_INFO  ''<馬連オッズ>
        Public TotalHyosuUmaren As String  ''馬連票数合計
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim OddsUmarenInfo(152)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 2042
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TorokuTosu = MidB2S(bBuff, 36, 2)
            SyussoTosu = MidB2S(bBuff, 38, 2)
            UmarenFlag = MidB2S(bBuff, 40, 1)
            For i = 0 To 152
                OddsUmarenInfo(i).SetDataB(MidB2B(bBuff, 41 + (13 * i), 13))
            Next i
            TotalHyosuUmaren = MidB2S(bBuff, 2030, 11)
            crlf = MidB2S(bBuff, 2041, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ８．オッズ（ワイド）****************************************
    '<ワイドオッズ>
    Public Structure ODDS_WIDE_INFO
        Public Kumi As String     ''組番
        Public OddsLow As String    ''最低オッズ
        Public OddsHigh As String    ''最高オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 4)
            OddsLow = MidB2S(bBuff, 5, 5)
            OddsHigh = MidB2S(bBuff, 10, 5)
            Ninki = MidB2S(bBuff, 15, 3)
        End Sub
    End Structure
    Public Structure JV_O3_ODDS_WIDE
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public TorokuTosu As String    ''登録頭数
        Public SyussoTosu As String    ''出走頭数
        Public WideFlag As String    ''発売フラグ　ワイド
        Public OddsWideInfo() As ODDS_WIDE_INFO ''<ワイドオッズ>
        Public TotalHyosuWide As String   ''ワイド票数合計
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim OddsWideInfo(152)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 2654
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TorokuTosu = MidB2S(bBuff, 36, 2)
            SyussoTosu = MidB2S(bBuff, 38, 2)
            WideFlag = MidB2S(bBuff, 40, 1)
            For i = 0 To 152
                OddsWideInfo(i).SetDataB(MidB2B(bBuff, 41 + (17 * i), 17))
            Next i
            TotalHyosuWide = MidB2S(bBuff, 2642, 11)
            crlf = MidB2S(bBuff, 2653, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ９．オッズ（馬単） ****************************************
    '<馬単オッズ>
    Public Structure ODDS_UMATAN_INFO
        Public Kumi As String     ''組番
        Public Odds As String     ''オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 4)
            Odds = MidB2S(bBuff, 5, 6)
            Ninki = MidB2S(bBuff, 11, 3)
        End Sub
    End Structure
    Public Structure JV_O4_ODDS_UMATAN
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public TorokuTosu As String    ''登録頭数
        Public SyussoTosu As String    ''出走頭数
        Public UmatanFlag As String    ''発売フラグ　馬単
        Public OddsUmatanInfo() As ODDS_UMATAN_INFO ''<馬単オッズ>
        Public TotalHyosuUmatan As String  ''馬単票数合計
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim OddsUmatanInfo(305)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 4031
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TorokuTosu = MidB2S(bBuff, 36, 2)
            SyussoTosu = MidB2S(bBuff, 38, 2)
            UmatanFlag = MidB2S(bBuff, 40, 1)
            For i = 0 To 305
                OddsUmatanInfo(i).SetDataB(MidB2B(bBuff, 41 + (13 * i), 13))
            Next i
            TotalHyosuUmatan = MidB2S(bBuff, 4019, 11)
            crlf = MidB2S(bBuff, 4030, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １０．オッズ（３連複）****************************************
    '<3連複オッズ>
    Public Structure ODDS_SANREN_INFO
        Public Kumi As String     ''組番
        Public Odds As String     ''オッズ
        Public Ninki As String     ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 6)
            Odds = MidB2S(bBuff, 7, 6)
            Ninki = MidB2S(bBuff, 13, 3)
        End Sub
    End Structure
    Public Structure JV_O5_ODDS_SANREN
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public TorokuTosu As String    ''登録頭数
        Public SyussoTosu As String    ''出走頭数
        Public SanrenpukuFlag As String   ''発売フラグ　3連複
        Public OddsSanrenInfo() As ODDS_SANREN_INFO ''<3連複オッズ>
        Public TotalHyosuSanrenpuku As String ''3連複票数合計
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim OddsSanrenInfo(815)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 12293
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TorokuTosu = MidB2S(bBuff, 36, 2)
            SyussoTosu = MidB2S(bBuff, 38, 2)
            SanrenpukuFlag = MidB2S(bBuff, 40, 1)
            For i = 0 To 815
                OddsSanrenInfo(i).SetDataB(MidB2B(bBuff, 41 + (15 * i), 15))
            Next i
            TotalHyosuSanrenpuku = MidB2S(bBuff, 12281, 11)
            crlf = MidB2S(bBuff, 12292, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １０－１．オッズ（３連単）****************************************
    '<3連単オッズ>
    Public Structure ODDS_SANRENTAN_INFO
        Public Kumi As String       ''組番
        Public Odds As String       ''オッズ
        Public Ninki As String      ''人気順
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumi = MidB2S(bBuff, 1, 6)
            Odds = MidB2S(bBuff, 7, 7)
            Ninki = MidB2S(bBuff, 14, 4)
        End Sub
    End Structure

    Public Structure JV_O6_ODDS_SANRENTAN
        Public head As RECORD_ID                            ''<レコードヘッダー>
        Public id As RACE_ID                                ''<競走識別情報>
        Public HappyoTime As MDHM                           ''発表月日時分
        Public TorokuTosu As String                         ''登録頭数
        Public SyussoTosu As String                         ''出走頭数
        Public SanrentanFlag As String                      ''発売フラグ　3連単
        Public OddsSanrentanInfo() As ODDS_SANRENTAN_INFO   ''<3連単オッズ>
        Public TotalHyosuSanrentan As String                ''3連単票数合計
        Public crlf As String                               ''レコード区切り

        '配列の初期化
        Public Sub Initialize()
            ReDim OddsSanrentanInfo(4895)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 83285
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TorokuTosu = MidB2S(bBuff, 36, 2)
            SyussoTosu = MidB2S(bBuff, 38, 2)
            SanrentanFlag = MidB2S(bBuff, 40, 1)
            For i = 0 To 4895
                OddsSanrentanInfo(i).SetDataB(MidB2B(bBuff, 41 + (17 * i), 17))
            Next i
            TotalHyosuSanrentan = MidB2S(bBuff, 83273, 11)
            crlf = MidB2S(bBuff, 83284, 2)
            bBuff = Nothing
        End Sub
    End Structure


    '****** １１．競走馬マスタ ****************************************
    '<３代血統情報>
    Public Structure KETTO3_INFO
        Public HansyokuNum As String   ''繁殖登録番号
        Public Bamei As String     ''馬名
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            HansyokuNum = MidB2S(bBuff, 1, 10)
            Bamei = MidB2S(bBuff, 11, 36)
        End Sub
    End Structure
    Public Structure JV_UM_UMA
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public KettoNum As String    ''血統登録番号
        Public DelKubun As String    ''競走馬抹消区分
        Public RegDate As YMD     ''競走馬登録年月日
        Public DelDate As YMD     ''競走馬抹消年月日
        Public BirthDate As YMD     ''生年月日
        Public Bamei As String     ''馬名
        Public BameiKana As String    ''馬名半角カナ
        Public BameiEng As String    ''馬名欧字
        Public ZaikyuFlag As String    ''JRA施設在きゅうフラグ
        Public Reserved As String    ''予備
        Public UmaKigoCD As String    ''馬記号コード
        Public SexCD As String     ''性別コード
        Public HinsyuCD As String    ''品種コード
        Public KeiroCD As String    ''毛色コード
        Public Ketto3Info() As KETTO3_INFO  ''<3代血統情報>
        Public TozaiCD As String    ''東西所属コード
        Public ChokyosiCode As String   ''調教師コード
        Public ChokyosiRyakusyo As String  ''調教師名略称
        Public Syotai As String     ''招待地域名
        Public BreederCode As String   ''生産者コード
        Public BreederName As String   ''生産者名
        Public SanchiName As String    ''産地名
        Public BanusiCode As String    ''馬主コード
        Public BanusiName As String    ''馬主名
        Public RuikeiHonsyoHeiti As String  ''平地本賞金累計
        Public RuikeiHonsyoSyogai As String  ''障害本賞金累計
        Public RuikeiFukaHeichi As String  ''平地付加賞金累計
        Public RuikeiFukaSyogai As String  ''障害付加賞金累計
        Public RuikeiSyutokuHeichi As String ''平地収得賞金累計
        Public RuikeiSyutokuSyogai As String ''障害収得賞金累計
        Public ChakuSogo As CHAKUKAISU3_INFO     ''総合着回数
        Public ChakuChuo As CHAKUKAISU3_INFO     ''中央合計着回数
        Public ChakuKaisuBa() As CHAKUKAISU3_INFO   ''馬場別着回数
        Public ChakuKaisuJyotai() As CHAKUKAISU3_INFO  ''馬場状態別着回数
        Public ChakuKaisuKyori() As CHAKUKAISU3_INFO  ''距離別着回数
        Public Kyakusitu() As String   ''脚質傾向
        Public RaceCount As String    ''登録レース数
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim Ketto3Info(13)
            ReDim ChakuKaisuBa(6)
            ReDim ChakuKaisuJyotai(11)
            ReDim Kyakusitu(3)
            ReDim ChakuKaisuKyori(5)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 1609
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            KettoNum = MidB2S(bBuff, 12, 10)
            DelKubun = MidB2S(bBuff, 22, 1)
            RegDate.SetDataB(MidB2B(bBuff, 23, 8))
            DelDate.SetDataB(MidB2B(bBuff, 31, 8))
            BirthDate.SetDataB(MidB2B(bBuff, 39, 8))
            Bamei = MidB2S(bBuff, 47, 36)
            BameiKana = MidB2S(bBuff, 83, 36)
            BameiEng = MidB2S(bBuff, 119, 60)
            ZaikyuFlag = MidB2S(bBuff, 179, 1)
            Reserved = MidB2S(bBuff, 180, 19)
            UmaKigoCD = MidB2S(bBuff, 199, 2)
            SexCD = MidB2S(bBuff, 201, 1)
            HinsyuCD = MidB2S(bBuff, 202, 1)
            KeiroCD = MidB2S(bBuff, 203, 2)
            For i = 0 To 13
                Ketto3Info(i).SetDataB(MidB2B(bBuff, 205 + (46 * i), 46))
            Next i
            TozaiCD = MidB2S(bBuff, 849, 1)
            ChokyosiCode = MidB2S(bBuff, 850, 5)
            ChokyosiRyakusyo = MidB2S(bBuff, 855, 8)
            Syotai = MidB2S(bBuff, 863, 20)
            BreederCode = MidB2S(bBuff, 883, 8)
            BreederName = MidB2S(bBuff, 891, 72)
            SanchiName = MidB2S(bBuff, 963, 20)
            BanusiCode = MidB2S(bBuff, 983, 6)
            BanusiName = MidB2S(bBuff, 989, 64)
            RuikeiHonsyoHeiti = MidB2S(bBuff, 1053, 9)
            RuikeiHonsyoSyogai = MidB2S(bBuff, 1062, 9)
            RuikeiFukaHeichi = MidB2S(bBuff, 1071, 9)
            RuikeiFukaSyogai = MidB2S(bBuff, 1080, 9)
            RuikeiSyutokuHeichi = MidB2S(bBuff, 1089, 9)
            RuikeiSyutokuSyogai = MidB2S(bBuff, 1098, 9)
            ChakuSogo.SetDataB(MidB2B(bBuff, 1107, 18))
            ChakuChuo.SetDataB(MidB2B(bBuff, 1125, 18))
            For i = 0 To 6
                ChakuKaisuBa(i).SetDataB(MidB2B(bBuff, 1143 + (18 * i), 18))
            Next i
            For i = 0 To 11
                ChakuKaisuJyotai(i).SetDataB(MidB2B(bBuff, 1269 + (18 * i), 18))
            Next i
            For i = 0 To 5
                ChakuKaisuKyori(i).SetDataB(MidB2B(bBuff, 1485 + (18 * i), 18))
            Next i
            For i = 0 To 3
                Kyakusitu(i) = MidB2S(bBuff, 1593 + (3 * i), 3)
            Next i
            RaceCount = MidB2S(bBuff, 1605, 3)
            crlf = MidB2S(bBuff, 1608, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １２．騎手マスタ ****************************************
    '<初騎乗情報>
    Public Structure HATUKIJYO_INFO
        Public Hatukijyoid As RACE_ID   ''年月日場回日R
        Public SyussoTosu As String    ''出走頭数
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        Public KakuteiJyuni As String   ''確定着順
        Public IJyoCD As String     ''異常区分コード
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Hatukijyoid.SetDataB(MidB2B(bBuff, 1, 16))
            SyussoTosu = MidB2S(bBuff, 17, 2)
            KettoNum = MidB2S(bBuff, 19, 10)
            Bamei = MidB2S(bBuff, 29, 36)
            KakuteiJyuni = MidB2S(bBuff, 65, 2)
            IJyoCD = MidB2S(bBuff, 67, 1)
        End Sub
    End Structure
    '<初勝利情報>
    Public Structure HATUSYORI_INFO
        Public Hatusyoriid As RACE_ID   ''年月日場回日R
        Public SyussoTosu As String    ''出走頭数
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Hatusyoriid.SetDataB(MidB2B(bBuff, 1, 16))
            SyussoTosu = MidB2S(bBuff, 17, 2)
            KettoNum = MidB2S(bBuff, 19, 10)
            Bamei = MidB2S(bBuff, 29, 36)
        End Sub
    End Structure
    Public Structure JV_KS_KISYU
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public KisyuCode As String    ''騎手コード
        Public DelKubun As String    ''騎手抹消区分
        Public IssueDate As YMD     ''騎手免許交付年月日
        Public DelDate As YMD     ''騎手免許抹消年月日
        Public BirthDate As YMD     ''生年月日
        Public KisyuName As String    ''騎手名漢字
        Public reserved As String    ''予備
        Public KisyuNameKana As String   ''騎手名半角カナ
        Public KisyuRyakusyo As String   ''騎手名略称
        Public KisyuNameEng As String   ''騎手名欧字
        Public SexCD As String     ''性別区分
        Public SikakuCD As String    ''騎乗資格コード
        Public MinaraiCD As String    ''騎手見習コード
        Public TozaiCD As String    ''騎手東西所属コード
        Public Syotai As String     ''招待地域名
        Public ChokyosiCode As String   ''所属調教師コード
        Public ChokyosiRyakusyo As String  ''所属調教師名略称
        Public HatuKiJyo() As HATUKIJYO_INFO   ''<初騎乗情報>
        Public HatuSyori() As HATUSYORI_INFO   ''<初勝利情報>
        Public SaikinJyusyo() As SAIKIN_JYUSYO_INFO  ''<最近重賞勝利情報>
        Public HonZenRuikei() As HON_ZEN_RUIKEISEI_INFO ''<本年・前年・累計成績情報>
        Public crlf As String   ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim HatuKiJyo(1)
            ReDim HatuSyori(1)
            ReDim SaikinJyusyo(2)
            ReDim HonZenRuikei(2)
        End Sub
        'データセット	
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 4173
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            KisyuCode = MidB2S(bBuff, 12, 5)
            DelKubun = MidB2S(bBuff, 17, 1)
            IssueDate.SetDataB(MidB2B(bBuff, 18, 8))
            DelDate.SetDataB(MidB2B(bBuff, 26, 8))
            BirthDate.SetDataB(MidB2B(bBuff, 34, 8))
            KisyuName = MidB2S(bBuff, 42, 34)
            reserved = MidB2S(bBuff, 76, 34)
            KisyuNameKana = MidB2S(bBuff, 110, 30)
            KisyuRyakusyo = MidB2S(bBuff, 140, 8)
            KisyuNameEng = MidB2S(bBuff, 148, 80)
            SexCD = MidB2S(bBuff, 228, 1)
            SikakuCD = MidB2S(bBuff, 229, 1)
            MinaraiCD = MidB2S(bBuff, 230, 1)
            TozaiCD = MidB2S(bBuff, 231, 1)
            Syotai = MidB2S(bBuff, 232, 20)
            ChokyosiCode = MidB2S(bBuff, 252, 5)
            ChokyosiRyakusyo = MidB2S(bBuff, 257, 8)
            For i = 0 To 1
                HatuKiJyo(i).SetDataB(MidB2B(bBuff, 265 + (67 * i), 67))
            Next i
            For i = 0 To 1
                HatuSyori(i).SetDataB(MidB2B(bBuff, 399 + (64 * i), 64))
            Next i
            For i = 0 To 2
                SaikinJyusyo(i).SetDataB(MidB2B(bBuff, 527 + (163 * i), 163))
            Next i
            For i = 0 To 2
                HonZenRuikei(i).SetDataB(MidB2B(bBuff, 1016 + (1052 * i), 1052))
            Next i
            crlf = MidB2S(bBuff, 4172, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １３．調教師マスタ ****************************************
    Public Structure JV_CH_CHOKYOSI
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public ChokyosiCode As String   ''調教師コード
        Public DelKubun As String    ''調教師抹消区分
        Public IssueDate As YMD      ''調教師免許交付年月日
        Public DelDate As YMD     ''調教師免許抹消年月日
        Public BirthDate As YMD     ''生年月日
        Public ChokyosiName As String   ''調教師名漢字
        Public ChokyosiNameKana As String  ''調教師名半角カナ
        Public ChokyosiRyakusyo As String  ''調教師名略称
        Public ChokyosiNameEng As String  ''調教師名欧字
        Public SexCD As String     ''性別区分
        Public TozaiCD As String    ''調教師東西所属コード
        Public Syotai As String     ''招待地域名
        Public SaikinJyusyo() As SAIKIN_JYUSYO_INFO  ''<最近重賞勝利情報>
        Public HonZenRuikei() As HON_ZEN_RUIKEISEI_INFO ''<本年・前年・累計成績情報>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim SaikinJyusyo(2)
            ReDim HonZenRuikei(2)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 3862
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            ChokyosiCode = MidB2S(bBuff, 12, 5)
            DelKubun = MidB2S(bBuff, 17, 1)
            IssueDate.SetDataB(MidB2B(bBuff, 18, 8))
            DelDate.SetDataB(MidB2B(bBuff, 26, 8))
            BirthDate.SetDataB(MidB2B(bBuff, 34, 8))
            ChokyosiName = MidB2S(bBuff, 42, 34)
            ChokyosiNameKana = MidB2S(bBuff, 76, 30)
            ChokyosiRyakusyo = MidB2S(bBuff, 106, 8)
            ChokyosiNameEng = MidB2S(bBuff, 114, 80)
            SexCD = MidB2S(bBuff, 194, 1)
            TozaiCD = MidB2S(bBuff, 195, 1)
            Syotai = MidB2S(bBuff, 196, 20)
            For i = 0 To 2
                SaikinJyusyo(i).SetDataB(MidB2B(bBuff, 216 + (163 * i), 163))
            Next i
            For i = 0 To 2
                HonZenRuikei(i).SetDataB(MidB2B(bBuff, 705 + (1052 * i), 1052))
            Next i
            crlf = MidB2S(bBuff, 3861, 2)
            bBuff = Nothing
        End Sub
    End Structure

    ''******１４．生産者マスタ ****************************************
    Public Structure JV_BR_BREEDER
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public BreederCode As String   ''生産者コード
        Public BreederName_Co As String   ''生産者名(法人格有)
        Public BreederName As String   ''生産者名(法人格無)
        Public BreederNameKana As String  ''生産者名半角カナ
        Public BreederNameEng As String   ''生産者名欧字
        Public Address As String    ''生産者住所自治省名
        Public HonRuikei() As SEI_RUIKEI_INFO ''<本年・累計成績情報>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim HonRuikei(1)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 545
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            BreederCode = MidB2S(bBuff, 12, 8)
            BreederName_Co = MidB2S(bBuff, 20, 72)
            BreederName = MidB2S(bBuff, 92, 72)
            BreederNameKana = MidB2S(bBuff, 164, 72)
            BreederNameEng = MidB2S(bBuff, 236, 168)
            Address = MidB2S(bBuff, 404, 20)
            For i = 0 To 1
                HonRuikei(i).SetDataB(MidB2B(bBuff, 424 + (60 * i), 60))
            Next i
            crlf = MidB2S(bBuff, 544, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １５．馬主マスタ ****************************************
    Public Structure JV_BN_BANUSI
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public BanusiCode As String    ''馬主コード
        Public BanusiName_Co As String    ''馬主名(法人格有)
        Public BanusiName As String    ''馬主名(法人格無)
        Public BanusiNameKana As String   ''馬主名半角カナ
        Public BanusiNameEng As String   ''馬主名欧字
        Public Fukusyoku As String    ''服色標示
        Public HonRuikei() As SEI_RUIKEI_INFO ''<本年・累計成績情報>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim HonRuikei(1)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 477
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            BanusiCode = MidB2S(bBuff, 12, 6)
            BanusiName_Co = MidB2S(bBuff, 18, 64)
            BanusiName = MidB2S(bBuff, 82, 64)
            BanusiNameKana = MidB2S(bBuff, 146, 50)
            BanusiNameEng = MidB2S(bBuff, 196, 100)
            Fukusyoku = MidB2S(bBuff, 296, 60)
            For i = 0 To 1
                HonRuikei(i).SetDataB(MidB2B(bBuff, 356 + (60 * i), 60))
            Next i
            crlf = MidB2S(bBuff, 476, 2)
            bBuff = Nothing
        End Sub
    End Structure

    ''****** １６．繁殖馬マスタ ****************************************
    Public Structure JV_HN_HANSYOKU
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public HansyokuNum As String   ''繁殖登録番号
        Public reserved As String    ''予備
        Public KettoNum As String    ''血統登録番号
        Public DelKubun As String    ''繁殖馬抹消区分(現在は予備として使用)
        Public Bamei As String     ''馬名
        Public BameiKana As String    ''馬名半角カナ
        Public BameiEng As String    ''馬名欧字
        Public BirthYear As String    ''生年
        Public SexCD As String     ''性別コード
        Public HinsyuCD As String    ''品種コード
        Public KeiroCD As String    ''毛色コード
        Public HansyokuMochiKubun As String  ''繁殖馬持込区分
        Public ImportYear As String    ''輸入年
        Public SanchiName As String    ''産地名
        Public HansyokuFNum As String   ''父馬繁殖登録番号
        Public HansyokuMNum As String   ''母馬繁殖登録番号
        Public crlf As String     ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 251
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            HansyokuNum = MidB2S(bBuff, 12, 10)
            reserved = MidB2S(bBuff, 22, 8)
            KettoNum = MidB2S(bBuff, 30, 10)
            DelKubun = MidB2S(bBuff, 40, 1)
            Bamei = MidB2S(bBuff, 41, 36)
            BameiKana = MidB2S(bBuff, 77, 40)
            BameiEng = MidB2S(bBuff, 117, 80)
            BirthYear = MidB2S(bBuff, 197, 4)
            SexCD = MidB2S(bBuff, 201, 1)
            HinsyuCD = MidB2S(bBuff, 202, 1)
            KeiroCD = MidB2S(bBuff, 203, 2)
            HansyokuMochiKubun = MidB2S(bBuff, 205, 1)
            ImportYear = MidB2S(bBuff, 206, 4)
            SanchiName = MidB2S(bBuff, 210, 20)
            HansyokuFNum = MidB2S(bBuff, 230, 10)
            HansyokuMNum = MidB2S(bBuff, 240, 10)
            crlf = MidB2S(bBuff, 250, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １７．産駒マスタ ****************************************
    Public Structure JV_SK_SANKU
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public KettoNum As String    ''血統登録番号
        Public BirthDate As YMD     ''生年月日
        Public SexCD As String     ''性別コード
        Public HinsyuCD As String    ''品種コード
        Public KeiroCD As String    ''毛色コード
        Public SankuMochiKubun As String  ''産駒持込区分
        Public ImportYear As String    ''輸入年
        Public BreederCode As String   ''生産者コード
        Public SanchiName As String    ''産地名
        Public HansyokuNum() As String   ''3代血統 繁殖登録番号
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim HansyokuNum(13)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 208
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            KettoNum = MidB2S(bBuff, 12, 10)
            BirthDate.SetDataB(MidB2B(bBuff, 22, 8))
            SexCD = MidB2S(bBuff, 30, 1)
            HinsyuCD = MidB2S(bBuff, 31, 1)
            KeiroCD = MidB2S(bBuff, 32, 2)
            SankuMochiKubun = MidB2S(bBuff, 34, 1)
            ImportYear = MidB2S(bBuff, 35, 4)
            BreederCode = MidB2S(bBuff, 39, 8)
            SanchiName = MidB2S(bBuff, 47, 20)
            For i = 0 To 13
                HansyokuNum(i) = MidB2S(bBuff, 67 + (10 * i), 10)
            Next i
            crlf = MidB2S(bBuff, 207, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １８．レコードマスタ ****************************************
    '<レコード保持馬情報>
    Public Structure RECUMA_INFO
        Public KettoNum As String    ''血統登録番号
        Public Bamei As String     ''馬名
        Public UmaKigoCD As String    ''馬記号コード
        Public SexCD As String     ''性別コード
        Public ChokyosiCode As String   ''調教師コード
        Public ChokyosiName As String   ''調教師名
        Public Futan As String     ''負担重量
        Public KisyuCode As String    ''騎手コード
        Public KisyuName As String    ''騎手名
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            KettoNum = MidB2S(bBuff, 1, 10)
            Bamei = MidB2S(bBuff, 11, 36)
            UmaKigoCD = MidB2S(bBuff, 47, 2)
            SexCD = MidB2S(bBuff, 49, 1)
            ChokyosiCode = MidB2S(bBuff, 50, 5)
            ChokyosiName = MidB2S(bBuff, 55, 34)
            Futan = MidB2S(bBuff, 89, 3)
            KisyuCode = MidB2S(bBuff, 92, 5)
            KisyuName = MidB2S(bBuff, 97, 34)
        End Sub
    End Structure
    Public Structure JV_RC_RECORD
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public RecInfoKubun As String   ''レコード識別区分
        Public id As RACE_ID     ''<競走識別情報>
        Public TokuNum As String    ''特別競走番号
        Public Hondai As String     ''競走名本題
        Public GradeCD As String    ''グレードコード
        Public SyubetuCD As String    ''競走種別コード
        Public Kyori As String     ''距離
        Public TrackCD As String    ''トラックコード
        Public RecKubun As String    ''レコード区分
        Public RecTime As String    ''レコードタイム
        Public TenkoBaba As TENKO_BABA_INFO  ''天候・馬場状態
        Public RecUmaInfo() As RECUMA_INFO  ''<レコード保持馬情報>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim RecUmaInfo(2)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 501
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            RecInfoKubun = MidB2S(bBuff, 12, 1)
            id.SetDataB(MidB2B(bBuff, 13, 16))
            TokuNum = MidB2S(bBuff, 29, 4)
            Hondai = MidB2S(bBuff, 33, 60)
            GradeCD = MidB2S(bBuff, 93, 1)
            SyubetuCD = MidB2S(bBuff, 94, 2)
            Kyori = MidB2S(bBuff, 96, 4)
            TrackCD = MidB2S(bBuff, 100, 2)
            RecKubun = MidB2S(bBuff, 102, 1)
            RecTime = MidB2S(bBuff, 103, 4)
            TenkoBaba.SetDataB(MidB2B(bBuff, 107, 3))
            For i = 0 To 2
                RecUmaInfo(i).SetDataB(MidB2B(bBuff, 110 + (130 * i), 130))
            Next i
            crlf = MidB2S(bBuff, 500, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** １９．坂路調教 ****************************************
    Public Structure JV_HC_HANRO
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public TresenKubun As String   ''トレセン区分
        Public ChokyoDate As YMD    ''調教年月日
        Public ChokyoTime As String    ''調教時刻
        Public KettoNum As String    ''血統登録番号
        Public HaronTime4 As String    ''4ハロンタイム合計(800M-0M)
        Public LapTime4 As String    ''ラップタイム(800M-600M)
        Public HaronTime3 As String    ''3ハロンタイム合計(600M-0M)
        Public LapTime3 As String    ''ラップタイム(600M-400M)
        Public HaronTime2 As String    ''2ハロンタイム合計(400M-0M)
        Public LapTime2 As String    ''ラップタイム(400M-200M)
        Public LapTime1 As String    ''ラップタイム(200M-0M)
        Public crlf As String     ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 60
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            TresenKubun = MidB2S(bBuff, 12, 1)
            ChokyoDate.SetDataB(MidB2B(bBuff, 13, 8))
            ChokyoTime = MidB2S(bBuff, 21, 4)
            KettoNum = MidB2S(bBuff, 25, 10)
            HaronTime4 = MidB2S(bBuff, 35, 4)
            LapTime4 = MidB2S(bBuff, 39, 3)
            HaronTime3 = MidB2S(bBuff, 42, 4)
            LapTime3 = MidB2S(bBuff, 46, 3)
            HaronTime2 = MidB2S(bBuff, 49, 4)
            LapTime2 = MidB2S(bBuff, 53, 3)
            LapTime1 = MidB2S(bBuff, 56, 3)
            crlf = MidB2S(bBuff, 59, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２０．馬体重 ****************************************
    '<馬体重情報>
    Public Structure BATAIJYU_INFO
        Public Umaban As String     ''馬番
        Public Bamei As String     ''馬名
        Public BaTaijyu As String    ''馬体重
        Public ZogenFugo As String    ''増減符号
        Public ZogenSa As String    ''増減差
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            Bamei = MidB2S(bBuff, 3, 36)
            BaTaijyu = MidB2S(bBuff, 39, 3)
            ZogenFugo = MidB2S(bBuff, 42, 1)
            ZogenSa = MidB2S(bBuff, 43, 3)
        End Sub
    End Structure
    Public Structure JV_WH_BATAIJYU
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public BataijyuInfo() As BATAIJYU_INFO ''<馬体重情報>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim BataijyuInfo(17)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 847
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            For i = 0 To 17
                BataijyuInfo(i).SetDataB(MidB2B(bBuff, 36 + (45 * i), 45))
            Next i
            crlf = MidB2S(bBuff, 846, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２１．天候馬場状態 ******************************************
    Public Structure JV_WE_WEATHER
        Public head As RECORD_ID     ''<レコードヘッダー>
        Public id As RACE_ID2      ''<競走識別情報２>
        Public HappyoTime As MDHM     ''発表月日時分
        Public HenkoID As String     ''変更識別
        Public TenkoBaba As TENKO_BABA_INFO   ''現在状態情報
        Public TenkoBabaBefore As TENKO_BABA_INFO   ''変更前状態情報
        Public crlf As String     ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 42
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 14))
            HappyoTime.SetDataB(MidB2B(bBuff, 26, 8))
            HenkoID = MidB2S(bBuff, 34, 1)
            TenkoBaba.SetDataB(MidB2B(bBuff, 35, 3))
            TenkoBabaBefore.SetDataB(MidB2B(bBuff, 38, 3))
            crlf = MidB2S(bBuff, 41, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２２．出走取消・競争除外 ****************************************
    Public Structure JV_AV_INFO
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public Umaban As String     ''馬番
        Public Bamei As String     ''馬名
        Public JiyuKubun As String    ''事由区分
        Public crlf As String     ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 78
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            Umaban = MidB2S(bBuff, 36, 2)
            Bamei = MidB2S(bBuff, 38, 36)
            JiyuKubun = MidB2S(bBuff, 74, 3)
            crlf = MidB2S(bBuff, 77, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '************ ２３．騎手変更 **************************************** 
    '<変更情報>
    Public Structure JC_INFO
        Public Futan As String     ''負担重量
        Public KisyuCode As String    ''騎手コード
        Public KisyuName As String    ''騎手名
        Public MinaraiCD As String    ''騎手見習コード
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Futan = MidB2S(bBuff, 1, 3)
            KisyuCode = MidB2S(bBuff, 4, 5)
            KisyuName = MidB2S(bBuff, 9, 34)
            MinaraiCD = MidB2S(bBuff, 43, 1)
        End Sub
    End Structure
    Public Structure JV_JC_INFO
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public HappyoTime As MDHM    ''発表月日時分
        Public Umaban As String     ''馬番
        Public Bamei As String     ''馬名
        Public JCInfoAfter As JC_INFO   ''<変更後情報>
        Public JCInfoBefore As JC_INFO   ''<変更前情報>
        Public crlf As String     ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 161
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            Umaban = MidB2S(bBuff, 36, 2)
            Bamei = MidB2S(bBuff, 38, 36)
            JCInfoAfter.SetDataB(MidB2B(bBuff, 74, 43))
            JCInfoBefore.SetDataB(MidB2B(bBuff, 117, 43))
            crlf = MidB2S(bBuff, 160, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '************ ２３－１．発走時刻変更 **************************************** 
    '<変更情報>
    Public Structure TC_INFO
        Public Ji As String  ''時
        Public Fun As String  ''分
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Ji = MidB2S(bBuff, 1, 2)
            Fun = MidB2S(bBuff, 3, 2)
        End Sub
    End Structure
    Public Structure JV_TC_INFO
        Public head As RECORD_ID  ''<レコードヘッダー>
        Public id As RACE_ID   ''<競走識別情報>
        Public HappyoTime As MDHM  ''発表月日時分
        Public TCInfoAfter As TC_INFO ''<変更後情報>
        Public TCInfoBefore As TC_INFO ''<変更前情報>
        Public crlf As String   ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 45
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)
            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            TCInfoAfter.SetDataB(MidB2B(bBuff, 36, 4))
            TCInfoBefore.SetDataB(MidB2B(bBuff, 40, 4))
            crlf = MidB2S(bBuff, 44, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '************ ２３－２．コース変更 **************************************** 
    '<変更情報>
    Public Structure CC_INFO
        Public Kyori As String   ''距離
        Public TruckCd As String  ''トラックコード
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kyori = MidB2S(bBuff, 1, 4)
            TruckCd = MidB2S(bBuff, 5, 2)
        End Sub
    End Structure
    Public Structure JV_CC_INFO
        Public head As RECORD_ID  ''<レコードヘッダー>
        Public id As RACE_ID   ''<競走識別情報>
        Public HappyoTime As MDHM  ''発表月日時分
        Public CCInfoAfter As CC_INFO ''<変更後情報>
        Public CCInfoBefore As CC_INFO ''<変更前情報>
        Public JiyuCd As String   ''事由コード
        Public crlf As String   ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 50
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)
            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            HappyoTime.SetDataB(MidB2B(bBuff, 28, 8))
            CCInfoAfter.SetDataB(MidB2B(bBuff, 36, 6))
            CCInfoBefore.SetDataB(MidB2B(bBuff, 42, 6))
            JiyuCd = MidB2S(bBuff, 48, 1)
            crlf = MidB2S(bBuff, 49, 2)
            bBuff = Nothing
        End Sub
    End Structure


    '****** ２４．データマイニング予想************************************
    '<マイニング予想>
    Public Structure DM_INFO
        Public Umaban As String     ''馬番
        Public DMTime As String     ''予想走破タイム
        Public DMGosaP As String    ''予想誤差(信頼度)＋
        Public DMGosaM As String    ''予想誤差(信頼度)－
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            DMTime = MidB2S(bBuff, 3, 5)
            DMGosaP = MidB2S(bBuff, 8, 4)
            DMGosaM = MidB2S(bBuff, 12, 4)
        End Sub
    End Structure
    Public Structure JV_DM_INFO
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID     ''<競走識別情報>
        Public MakeHM As HM      ''データ作成時分
        Public DMInfo() As DM_INFO    ''<マイニング予想>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim DMInfo(17)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化							
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 303
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            MakeHM.SetDataB(MidB2B(bBuff, 28, 4))
            For i = 0 To 17
                DMInfo(i).SetDataB(MidB2B(bBuff, 32 + (15 * i), 15))
            Next i
            crlf = MidB2S(bBuff, 302, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２５．開催スケジュール************************************
    '<重賞案内>
    Public Structure JYUSYO_INFO
        Public TokuNum As String    ''特別競走番号
        Public Hondai As String     ''競走名本題
        Public Ryakusyo10 As String    ''競走名略称10字
        Public Ryakusyo6 As String    ''競走名略称6字
        Public Ryakusyo3 As String    ''競走名略称3字
        Public Nkai As String     ''重賞回次[第N回]
        Public GradeCD As String    ''グレードコード
        Public SyubetuCD As String    ''競走種別コード
        Public KigoCD As String     ''競走記号コード
        Public JyuryoCD As String    ''重量種別コード
        Public Kyori As String     ''距離
        Public TrackCD As String    ''トラックコード
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            TokuNum = MidB2S(bBuff, 1, 4)
            Hondai = MidB2S(bBuff, 5, 60)
            Ryakusyo10 = MidB2S(bBuff, 65, 20)
            Ryakusyo6 = MidB2S(bBuff, 85, 12)
            Ryakusyo3 = MidB2S(bBuff, 97, 6)
            Nkai = MidB2S(bBuff, 103, 3)
            GradeCD = MidB2S(bBuff, 106, 1)
            SyubetuCD = MidB2S(bBuff, 107, 2)
            KigoCD = MidB2S(bBuff, 109, 3)
            JyuryoCD = MidB2S(bBuff, 112, 1)
            Kyori = MidB2S(bBuff, 113, 4)
            TrackCD = MidB2S(bBuff, 117, 2)
        End Sub
    End Structure
    Public Structure JV_YS_SCHEDULE
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public id As RACE_ID2     ''<競走識別情報２>
        Public YoubiCD As String    ''曜日コード
        Public JyusyoInfo() As JYUSYO_INFO  ''<重賞案内>
        Public crlf As String     ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim JyusyoInfo(2)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化	
            Dim bBuff As Byte()
            Dim i As Integer
            Dim bSize As Long
            bSize = 382
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 14))
            YoubiCD = MidB2S(bBuff, 26, 1)
            For i = 0 To 2
                JyusyoInfo(i).SetDataB(MidB2B(bBuff, 27 + (118 * i), 118))
            Next i
            crlf = MidB2S(bBuff, 381, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２６．競走馬市場取引価格 ****************************************
    Public Structure JV_HS_SALE
        Public head As RECORD_ID         ''<レコードヘッダー>
        Public KettoNum As String        ''血統登録番号
        Public HansyokuFNum As String    ''父馬繁殖登録番号
        Public HansyokuMNum As String    ''母馬繁殖登録番号
        Public BirthYear As String       ''生年
        Public SaleCode As String        ''主催者・市場コード
        Public SaleHostName As String    ''主催者名称
        Public SaleName As String        ''市場の名称
        Public FromDate As YMD           ''市場の開催期間(開始日)
        Public ToDate As YMD             ''市場の開催期間(終了日)
        Public Barei As String          ''取引時の競走馬の年齢
        Public Price As String          ''取引価格
        Public crlf As String            ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 200
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            KettoNum = MidB2S(bBuff, 12, 10)
            HansyokuFNum = MidB2S(bBuff, 22, 10)
            HansyokuMNum = MidB2S(bBuff, 32, 10)
            BirthYear = MidB2S(bBuff, 42, 4)
            SaleCode = MidB2S(bBuff, 46, 6)
            SaleHostName = MidB2S(bBuff, 52, 40)
            SaleName = MidB2S(bBuff, 92, 80)
            FromDate.SetDataB(MidB2B(bBuff, 172, 8))
            ToDate.SetDataB(MidB2B(bBuff, 180, 8))
            Barei = MidB2S(bBuff, 188, 1)
            Price = MidB2S(bBuff, 189, 10)
            crlf = MidB2S(bBuff, 199, 2)
            bBuff = Nothing
        End Sub
    End Structure

    ''****** ２７．馬名の意味由来 ****************************************
    Public Structure JV_HY_BAMEIORIGIN
        Public head As RECORD_ID       ''<レコードヘッダー>
        Public KettoNum As String      ''血統登録番号
        Public Bamei As String         ''馬名
        Public Origin As String        ''馬名の意味由来
        Public crlf As String          ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 123
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            KettoNum = MidB2S(bBuff, 12, 10)
            Bamei = MidB2S(bBuff, 22, 36)
            Origin = MidB2S(bBuff, 58, 64)
            crlf = MidB2S(bBuff, 122, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２８．出走別着度数 ****************************************

    '<出走別着度数 競走馬情報>
    Public Structure JV_CK_UMA
        Public KettoNum As String                         ''血統登録番号
        Public Bamei As String                            ''馬名
        Public RuikeiHonsyoHeiti As String                ''平地本賞金累計
        Public RuikeiHonsyoSyogai As String               ''障害本賞金累計
        Public RuikeiFukaHeichi As String                 ''平地付加賞金累計
        Public RuikeiFukaSyogai As String                 ''障害付加賞金累計
        Public RuikeiSyutokuHeichi As String              ''平地収得賞金累計
        Public RuikeiSyutokuSyogai As String              ''障害収得賞金累計
        Public ChakuSogo As CHAKUKAISU3_INFO              ''総合着回数
        Public ChakuChuo As CHAKUKAISU3_INFO              ''中央合計着回数
        Public ChakuKaisuBa() As CHAKUKAISU3_INFO         ''馬場別着回数
        Public ChakuKaisuJyotai() As CHAKUKAISU3_INFO     ''馬場状態別着回数
        Public ChakuKaisuSibaKyori() As CHAKUKAISU3_INFO  ''芝距離別着回数
        Public ChakuKaisuDirtKyori() As CHAKUKAISU3_INFO  ''ダート距離別着回数
        Public ChakuKaisuJyoSiba() As CHAKUKAISU3_INFO    ''競馬場別芝着回数
        Public ChakuKaisuJyoDirt() As CHAKUKAISU3_INFO    ''競馬場別ダート着回数
        Public ChakuKaisuJyoSyogai() As CHAKUKAISU3_INFO  ''競馬場別障害着回数
        Public Kyakusitu() As String                      ''脚質傾向
        Public RaceCount As String                        ''登録レース数
        '配列の初期化
        Public Sub Initialize()
            ReDim ChakuKaisuBa(6)
            ReDim ChakuKaisuJyotai(11)
            ReDim ChakuKaisuSibaKyori(8)
            ReDim ChakuKaisuDirtKyori(8)
            ReDim ChakuKaisuJyoSiba(9)
            ReDim ChakuKaisuJyoDirt(9)
            ReDim ChakuKaisuJyoSyogai(9)
            ReDim Kyakusitu(3)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            KettoNum = MidB2S(bBuff, 1, 10)
            Bamei = MidB2S(bBuff, 11, 36)
            RuikeiHonsyoHeiti = MidB2S(bBuff, 47, 9)
            RuikeiHonsyoSyogai = MidB2S(bBuff, 56, 9)
            RuikeiFukaHeichi = MidB2S(bBuff, 65, 9)
            RuikeiFukaSyogai = MidB2S(bBuff, 74, 9)
            RuikeiSyutokuHeichi = MidB2S(bBuff, 83, 9)
            RuikeiSyutokuSyogai = MidB2S(bBuff, 92, 9)
            ChakuSogo.SetDataB(MidB2B(bBuff, 101, 18))
            ChakuChuo.SetDataB(MidB2B(bBuff, 119, 18))
            Dim i As Integer = 0
            For i = 0 To 6
                ChakuKaisuBa(i).SetDataB(MidB2B(bBuff, 137 + 18 * i, 18))
            Next i
            For i = 0 To 11
                ChakuKaisuJyotai(i).SetDataB(MidB2B(bBuff, 263 + 18 * i, 18))
            Next i
            For i = 0 To 8
                ChakuKaisuSibaKyori(i).SetDataB(MidB2B(bBuff, 479 + 18 * i, 18))
            Next i
            For i = 0 To 8
                ChakuKaisuDirtKyori(i).SetDataB(MidB2B(bBuff, 641 + 18 * i, 18))
            Next i
            For i = 0 To 9
                ChakuKaisuJyoSiba(i).SetDataB(MidB2B(bBuff, 803 + 18 * i, 18))
            Next i
            For i = 0 To 9
                ChakuKaisuJyoDirt(i).SetDataB(MidB2B(bBuff, 983 + 18 * i, 18))
            Next i
            For i = 0 To 9
                ChakuKaisuJyoSyogai(i).SetDataB(MidB2B(bBuff, 1163 + 18 * i, 18))
            Next i
            For i = 0 To 3
                Kyakusitu(i) = MidB2S(bBuff, 1343 + (3 * i), 3)
            Next i
            RaceCount = MidB2S(bBuff, 1355, 3)
        End Sub
    End Structure

    '<出走別着度数 本年・累計成績情報>
    Public Structure JV_CK_HON_RUIKEISEI_INFO
        Public SetYear As String                          ''設定年
        Public HonSyokinHeichi As String                  ''平地本賞金合計
        Public HonSyokinSyogai As String                  ''障害本賞金合計
        Public FukaSyokinHeichi As String                 ''平地付加賞金合計
        Public FukaSyokinSyogai As String                 ''障害付加賞金合計
        Public ChakuKaisuSiba As CHAKUKAISU5_INFO         ''芝着回数
        Public ChakuKaisuDirt As CHAKUKAISU5_INFO         ''ダート着回数
        Public ChakuKaisuSyogai As CHAKUKAISU4_INFO       ''障害着回数
        Public ChakuKaisuSibaKyori() As CHAKUKAISU4_INFO ''芝距離別着回数
        Public ChakuKaisuDirtKyori() As CHAKUKAISU4_INFO ''ダート距離別着回数
        Public ChakuKaisuJyoSiba() As CHAKUKAISU4_INFO   ''競馬場別芝着回数
        Public ChakuKaisuJyoDirt() As CHAKUKAISU4_INFO   ''競馬場別ダート着回数
        Public ChakuKaisuJyoSyogai() As CHAKUKAISU3_INFO ''競馬場別障害着回数
        '配列の初期化
        Public Sub Initialize()
            ReDim ChakuKaisuSibaKyori(8)
            ReDim ChakuKaisuDirtKyori(8)
            ReDim ChakuKaisuJyoSiba(9)
            ReDim ChakuKaisuJyoDirt(9)
            ReDim ChakuKaisuJyoSyogai(9)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            SetYear = MidB2S(bBuff, 1, 4)
            HonSyokinHeichi = MidB2S(bBuff, 5, 10)
            HonSyokinSyogai = MidB2S(bBuff, 15, 10)
            FukaSyokinHeichi = MidB2S(bBuff, 25, 10)
            FukaSyokinSyogai = MidB2S(bBuff, 35, 10)
            ChakuKaisuSiba.SetDataB(MidB2B(bBuff, 45, 30))
            ChakuKaisuDirt.SetDataB(MidB2B(bBuff, 75, 30))
            ChakuKaisuSyogai.SetDataB(MidB2B(bBuff, 105, 24))
            Dim i As Integer = 0
            For i = 0 To 8
                ChakuKaisuSibaKyori(i).SetDataB(MidB2B(bBuff, 129 + 24 * i, 24))
            Next i
            For i = 0 To 8
                ChakuKaisuDirtKyori(i).SetDataB(MidB2B(bBuff, 345 + 24 * i, 24))
            Next i
            For i = 0 To 9
                ChakuKaisuJyoSiba(i).SetDataB(MidB2B(bBuff, 561 + 24 * i, 24))
            Next i
            For i = 0 To 9
                ChakuKaisuJyoDirt(i).SetDataB(MidB2B(bBuff, 801 + 24 * i, 24))
            Next i
            For i = 0 To 9
                ChakuKaisuJyoSyogai(i).SetDataB(MidB2B(bBuff, 1041 + 18 * i, 18))
            Next i
        End Sub
    End Structure

    '<出走別着度数 騎手情報>
    Public Structure JV_CK_KISYU
        Public KisyuCode As String                 ''騎手コード
        Public KisyuName As String                 ''騎手名漢字
        Public HonRuikei() As JV_CK_HON_RUIKEISEI_INFO ''<本年・累計成績情報>
        '配列の初期化
        Public Sub Initialize()
            ReDim HonRuikei(1)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            KisyuCode = MidB2S(bBuff, 1, 5)
            KisyuName = MidB2S(bBuff, 6, 34)
            Dim i As Integer = 0
            For i = 0 To 1
                HonRuikei(i).SetDataB(MidB2B(bBuff, 40 + 1220 * i, 1220))
            Next i
        End Sub
    End Structure

    '<出走別着度数 調教師情報>
    Public Structure JV_CK_CHOKYOSI
        Public ChokyosiCode As String              ''調教師コード
        Public ChokyosiName As String              ''調教師名漢字
        Public HonRuikei() As JV_CK_HON_RUIKEISEI_INFO ''<本年・累計成績情報>
        '配列の初期化
        Public Sub Initialize()
            ReDim HonRuikei(1)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            ChokyosiCode = MidB2S(bBuff, 1, 5)
            ChokyosiName = MidB2S(bBuff, 6, 34)
            Dim i As Integer = 0
            For i = 0 To 1
                HonRuikei(i).SetDataB(MidB2B(bBuff, 40 + 1220 * i, 1220))
            Next i
        End Sub
    End Structure

    '<出走別着度数 馬主情報>
    Public Structure JV_CK_BANUSI
        Public BanusiCode As String                ''馬主コード
        Public BanusiName_Co As String             ''馬主名（法人格有）
        Public BanusiName As String                ''馬主名（法人格無）
        Public HonRuikei() As SEI_RUIKEI_INFO     ''<本年・累計成績情報>
        '配列の初期化
        Public Sub Initialize()
            ReDim HonRuikei(1)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            BanusiCode = MidB2S(bBuff, 1, 6)
            BanusiName_Co = MidB2S(bBuff, 7, 64)
            BanusiName = MidB2S(bBuff, 71, 64)
            Dim i As Integer = 0
            For i = 0 To 1
                HonRuikei(i).SetDataB(MidB2B(bBuff, 135 + 60 * i, 60))
            Next i
        End Sub
    End Structure

    '<出走別着度数 生産者情報>
    Public Structure JV_CK_BREEDER
        Public BreederCode As String               ''生産者コード
        Public BreederName_Co As String            ''生産者名（法人格有）
        Public BreederName As String               ''生産者名（法人格無）
        Public HonRuikei() As SEI_RUIKEI_INFO     ''<本年・累計成績情報>
        '配列の初期化
        Public Sub Initialize()
            ReDim HonRuikei(1)
        End Sub
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Initialize()      ''配列の初期化
            BreederCode = MidB2S(bBuff, 1, 8)
            BreederName_Co = MidB2S(bBuff, 9, 72)
            BreederName = MidB2S(bBuff, 81, 72)
            Dim i As Integer = 0
            For i = 0 To 1
                HonRuikei(i).SetDataB(MidB2B(bBuff, 153 + 60 * i, 60))
            Next i
        End Sub
    End Structure

    Public Structure JV_CK_CHAKU
        Public head As RECORD_ID                   ''<レコードヘッダー>
        Public id As RACE_ID                       ''<競走識別情報１>
        Public UmaChaku As JV_CK_UMA               ''<出走別着度数 競走馬情報>
        Public KisyuChaku As JV_CK_KISYU           ''<出走別着度数 騎手情報>
        Public ChokyoChaku As JV_CK_CHOKYOSI       ''<出走別着度数 調教師情報>
        Public BanusiChaku As JV_CK_BANUSI         ''<出走別着度数 馬主情報>
        Public BreederChaku As JV_CK_BREEDER       ''<出走別着度数 生産者情報>
        Public crlf As String                      ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 6870
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            UmaChaku.SetDataB(MidB2B(bBuff, 28, 1357))
            KisyuChaku.SetDataB(MidB2B(bBuff, 1385, 2479))
            ChokyoChaku.SetDataB(MidB2B(bBuff, 3864, 2479))
            BanusiChaku.SetDataB(MidB2B(bBuff, 6343, 254))
            BreederChaku.SetDataB(MidB2B(bBuff, 6597, 272))
            crlf = MidB2S(bBuff, 6869, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ２９．系統情報 ****************************************
    Public Structure JV_BT_KEITO
        Public head As RECORD_ID       ''<レコードヘッダー>
        Public HansyokuNum As String   ''繁殖登録番号
        Public KeitoId As String       ''系統ID
        Public KeitoName As String     ''系統名
        Public KeitoEx As String       ''系統説明
        Public crlf As String          ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 6889
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            HansyokuNum = MidB2S(bBuff, 12, 10)
            KeitoId = MidB2S(bBuff, 22, 30)
            KeitoName = MidB2S(bBuff, 52, 36)
            KeitoEx = MidB2S(bBuff, 88, 6800)
            crlf = MidB2S(bBuff, 6888, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ３０．コース情報 ****************************************
    Public Structure JV_CS_COURSE
        Public head As RECORD_ID       ''<レコードヘッダー>
        Public JyoCD As String         ''競馬場コード
        Public Kyori As String         ''距離
        Public TrackCD As String       ''トラックコード
        Public KaishuDate As YMD       ''コース改修年月日
        Public CourseEx As String      ''コース説明
        Public crlf As String          ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 6829
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            JyoCD = MidB2S(bBuff, 12, 2)
            Kyori = MidB2S(bBuff, 14, 4)
            TrackCD = MidB2S(bBuff, 18, 2)
            KaishuDate.SetDataB(MidB2B(bBuff, 20, 8))
            CourseEx = MidB2S(bBuff, 28, 6800)
            crlf = MidB2S(bBuff, 6828, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ３１．対戦型データマイニング予想************************************
    '<マイニング予想>
    Public Structure TM_INFO
        Public Umaban As String    ''馬番
        Public TMScore As String    ''予測スコア
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Umaban = MidB2S(bBuff, 1, 2)
            TMScore = MidB2S(bBuff, 3, 4)
        End Sub
    End Structure
    Public Structure JV_TM_INFO
        Public head As RECORD_ID      ''<レコードヘッダー>
        Public id As RACE_ID          ''<競走識別情報>
        Public MakeHM As HM           ''データ作成時分
        Public TMInfo() As TM_INFO    ''<マイニング予想>
        Public crlf As String         ''レコード区切り
        '配列の初期化
        Public Sub Initialize()
            ReDim TMInfo(17)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化							
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 141
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            MakeHM.SetDataB(MidB2B(bBuff, 28, 4))
            For i = 0 To 17
                TMInfo(i).SetDataB(MidB2B(bBuff, 32 + (6 * i), 6))
            Next i
            crlf = MidB2S(bBuff, 140, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ３２．重勝式(WIN5)************************************
    '<重勝式対象レース情報>
    Public Structure WF_RACE_INFO
        Public JyoCD As String     ''競馬場コード
        Public Kaiji As String     ''開催回[第N回]
        Public Nichiji As String   ''開催日目[N日目]
        Public RaceNum As String   ''レース番号
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            JyoCD = MidB2S(bBuff, 1, 2)
            Kaiji = MidB2S(bBuff, 3, 2)
            Nichiji = MidB2S(bBuff, 5, 2)
            RaceNum = MidB2S(bBuff, 7, 2)
        End Sub
    End Structure

    '<有効票数情報>
    Public Structure WF_YUKO_HYO_INFO
        Public Yuko_Hyo As String     ''有効票数
        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Yuko_Hyo = MidB2S(bBuff, 1, 11)
        End Sub
    End Structure

    '<重勝式払戻情報>
    Public Structure WF_PAY_INFO
        Public Kumiban As String     ''組番
        Public Pay As String         ''重勝式払戻金
        Public Tekichu_Hyo As String ''的中票数

        'データセット
        Public Sub SetDataB(ByVal bBuff As Byte())
            Kumiban = MidB2S(bBuff, 1, 10)
            Pay = MidB2S(bBuff, 11, 9)
            Tekichu_Hyo = MidB2S(bBuff, 20, 10)

        End Sub
    End Structure

    Public Structure JV_WF_INFO
        Public head As RECORD_ID                   ''<レコードヘッダー>
        Public KaisaiDate As YMD                   ''開催年月日
        Public reserved1 As String                 ''予備
        Public WFRaceInfo() As WF_RACE_INFO        ''<重勝式対象レース情報>
        Public reserved2 As String                 ''予備
        Public Hatsubai_Hyo As String              ''重勝式発売票数
        Public WFYukoHyoInfo() As WF_YUKO_HYO_INFO ''<有効票数情報>
        Public HenkanFlag As String                ''返還フラグ
        Public FuseiritsuFlag As String            ''不成立フラグ
        Public TekichunashiFlag As String          ''的中無フラグ
        Public COShoki As String                   ''キャリーオーバー金額初期
        Public COZanDaka As String                 ''キャリーオーバー金額残高
        Public WFPayInfo() As WF_PAY_INFO          ''<重勝式払戻情報>
        Public crlf As String                      ''レコード区切り

        '配列の初期化
        Public Sub Initialize()
            ReDim WFRaceInfo(4)
            ReDim WFYukoHyoInfo(4)
            ReDim WFPayInfo(242)
        End Sub
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Initialize()      ''配列の初期化
            Dim i As Integer
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 7215
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            KaisaiDate.SetDataB(MidB2B(bBuff, 12, 8))
            reserved1 = MidB2S(bBuff, 20, 2)

            For i = 0 To 4
                WFRaceInfo(i).SetDataB(MidB2B(bBuff, 22 + (8 * i), 8))
            Next i

            reserved2 = MidB2S(bBuff, 62, 6)
            Hatsubai_Hyo = MidB2S(bBuff, 68, 11)

            For i = 0 To 4
                WFYukoHyoInfo(i).SetDataB(MidB2B(bBuff, 79 + (11 * i), 11))
            Next i

            HenkanFlag = MidB2S(bBuff, 134, 1)
            FuseiritsuFlag = MidB2S(bBuff, 135, 1)
            TekichunashiFlag = MidB2S(bBuff, 136, 1)
            COShoki = MidB2S(bBuff, 137, 15)
            COZanDaka = MidB2S(bBuff, 152, 15)

            For i = 0 To 242
                WFPayInfo(i).SetDataB(MidB2B(bBuff, 167 + (29 * i), 29))
            Next i

            crlf = MidB2S(bBuff, 7214, 2)
            bBuff = Nothing

        End Sub
    End Structure

    '****** ３３．競走馬除外情報************************************
    Public Structure JV_JG_JOGAIBA
        Public head As RECORD_ID            ''<レコードヘッダー>
        Public id As RACE_ID                ''<競走識別情報>
        Public KettoNum As String           ''血統登録番号
        Public Bamei As String              ''馬名
        Public ShutsubaTohyoJun As String   ''出馬投票受付順番
        Public ShussoKubun As String        ''出走区分
        Public JogaiJotaiKubun As String    ''除外状態区分
        Public crlf As String               ''レコード区切り

        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 80
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            id.SetDataB(MidB2B(bBuff, 12, 16))
            KettoNum = MidB2S(bBuff, 28, 10)
            Bamei = MidB2S(bBuff, 38, 36)
            ShutsubaTohyoJun = MidB2S(bBuff, 74, 3)
            ShussoKubun = MidB2S(bBuff, 77, 1)
            JogaiJotaiKubun = MidB2S(bBuff, 78, 1)
            crlf = MidB2S(bBuff, 79, 2)
            bBuff = Nothing
        End Sub
    End Structure

    '****** ３４．ウッドチップ調教 ****************************************
    Public Structure JV_WC_WOOD
        Public head As RECORD_ID    ''<レコードヘッダー>
        Public TresenKubun As String   ''トレセン区分
        Public ChokyoDate As YMD    ''調教年月日
        Public ChokyoTime As String    ''調教時刻
        Public KettoNum As String    ''血統登録番号
        Public Course As String    ''コース
        Public BabaAround As String    ''馬場周り
        Public reserved As String    ''予備
        Public HaronTime10 As String    ''10ハロンタイム合計(2000M-0M)
        Public LapTime10 As String    ''ラップタイム(2000M-1800M)
        Public HaronTime9 As String    ''9ハロンタイム合計(1800M-0M)
        Public LapTime9 As String    ''ラップタイム(1800M-1600M)
        Public HaronTime8 As String    ''8ハロンタイム合計(1600M-0M)
        Public LapTime8 As String    ''ラップタイム1600M-1400M)
        Public HaronTime7 As String    ''7ハロンタイム合計(1400M-0M)
        Public LapTime7 As String    ''ラップタイム(1400M-1200M)
        Public HaronTime6 As String    ''6ハロンタイム合計(1200M-0M)
        Public LapTime6 As String    ''ラップタイム(1200M-1000M)
        Public HaronTime5 As String    ''5ハロンタイム合計(1000M-0M)
        Public LapTime5 As String    ''ラップタイム(1000M-800M)
        Public HaronTime4 As String    ''4ハロンタイム合計(800M-0M)
        Public LapTime4 As String    ''ラップタイム(800M-600M)
        Public HaronTime3 As String    ''3ハロンタイム合計(600M-0M)
        Public LapTime3 As String    ''ラップタイム(600M-400M)
        Public HaronTime2 As String    ''2ハロンタイム合計(400M-0M)
        Public LapTime2 As String    ''ラップタイム(400M-200M)
        Public LapTime1 As String    ''ラップタイム(200M-0M)
        Public crlf As String     ''レコード区切り
        'データセット
        Public Sub SetData(ByRef strBuff As String)
            Dim bBuff As Byte()
            Dim bSize As Long
            bSize = 105
            bBuff = New Byte(bSize) {}
            bBuff = Str2Byte(strBuff)

            head.SetDataB(MidB2B(bBuff, 1, 11))
            TresenKubun = MidB2S(bBuff, 12, 1)
            ChokyoDate.SetDataB(MidB2B(bBuff, 13, 8))
            ChokyoTime = MidB2S(bBuff, 21, 4)
            KettoNum = MidB2S(bBuff, 25, 10)
            Course = MidB2S(bBuff, 35, 1)
            BabaAround = MidB2S(bBuff, 36, 1)
            reserved = MidB2S(bBuff, 37, 1)
            HaronTime10 = MidB2S(bBuff, 38, 4)
            LapTime10 = MidB2S(bBuff, 42, 3)
            HaronTime9 = MidB2S(bBuff, 45, 4)
            LapTime9 = MidB2S(bBuff, 49, 3)
            HaronTime8 = MidB2S(bBuff, 52, 4)
            LapTime8 = MidB2S(bBuff, 56, 3)
            HaronTime7 = MidB2S(bBuff, 59, 4)
            LapTime7 = MidB2S(bBuff, 63, 3)
            HaronTime6 = MidB2S(bBuff, 66, 4)
            LapTime6 = MidB2S(bBuff, 70, 3)
            HaronTime5 = MidB2S(bBuff, 73, 4)
            LapTime5 = MidB2S(bBuff, 77, 3)
            HaronTime4 = MidB2S(bBuff, 80, 4)
            LapTime4 = MidB2S(bBuff, 84, 3)
            HaronTime3 = MidB2S(bBuff, 87, 4)
            LapTime3 = MidB2S(bBuff, 91, 3)
            HaronTime2 = MidB2S(bBuff, 94, 4)
            LapTime2 = MidB2S(bBuff, 98, 3)
            LapTime1 = MidB2S(bBuff, 101, 3)
            crlf = MidB2S(bBuff, 104, 2)
            bBuff = Nothing
        End Sub
    End Structure

End Module
