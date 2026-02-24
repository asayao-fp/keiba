'========================================================================
'  JRA-VAN Data Lab. サンプルプログラム１(Form2.vb)
'
'
'   作成: JRA-VAN ソフトウェア工房  2003年4月22日
'
'========================================================================
'   (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
'========================================================================
Imports System
Imports System.Text

Public Class frmJVLinkDialog
    Inherits System.Windows.Forms.Form

#Region " Windows フォーム デザイナで生成されたコード "

    Public Sub New()
        MyBase.New()

        ' この呼び出しは Windows フォーム デザイナで必要です。
        InitializeComponent()

        ' InitializeComponent() 呼び出しの後に初期化を追加します。

    End Sub

    ' Form は dispose をオーバーライドしてコンポーネント一覧を消去します。
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ' Windows フォーム デザイナで必要です。
    Private components As System.ComponentModel.IContainer

    ' メモ : 以下のプロシージャは、Windows フォーム デザイナで必要です。
    ' Windows フォーム デザイナを使って変更してください。  
    ' コード エディタは使用しないでください。
    Friend WithEvents lblFromDate As System.Windows.Forms.Label
    Friend WithEvents lblDataSpec As System.Windows.Forms.Label
    Friend WithEvents tmrJVStatus As System.Windows.Forms.Timer
    Friend WithEvents cmdStart As System.Windows.Forms.Button
    Friend WithEvents lblProgressBar2 As System.Windows.Forms.Label
    Friend WithEvents lblProgressBar1 As System.Windows.Forms.Label
    Friend WithEvents progressBar2 As System.Windows.Forms.ProgressBar
    Friend WithEvents cmdCancel As System.Windows.Forms.Button
    Friend WithEvents progressBar1 As System.Windows.Forms.ProgressBar
    Friend WithEvents grpRadioBtn As System.Windows.Forms.GroupBox
    Friend WithEvents rbtSetup As System.Windows.Forms.RadioButton
    Friend WithEvents rbtNormal As System.Windows.Forms.RadioButton
    Friend WithEvents txtFromDate As System.Windows.Forms.TextBox
    Friend WithEvents txtDataSpec As System.Windows.Forms.TextBox
    Friend WithEvents rbtIsthisweek As System.Windows.Forms.RadioButton
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        Me.components = New System.ComponentModel.Container()
        Me.cmdStart = New System.Windows.Forms.Button()
        Me.lblProgressBar2 = New System.Windows.Forms.Label()
        Me.lblProgressBar1 = New System.Windows.Forms.Label()
        Me.progressBar2 = New System.Windows.Forms.ProgressBar()
        Me.cmdCancel = New System.Windows.Forms.Button()
        Me.progressBar1 = New System.Windows.Forms.ProgressBar()
        Me.tmrJVStatus = New System.Windows.Forms.Timer(Me.components)
        Me.grpRadioBtn = New System.Windows.Forms.GroupBox()
        Me.rbtSetup = New System.Windows.Forms.RadioButton()
        Me.rbtIsthisweek = New System.Windows.Forms.RadioButton()
        Me.rbtNormal = New System.Windows.Forms.RadioButton()
        Me.txtFromDate = New System.Windows.Forms.TextBox()
        Me.txtDataSpec = New System.Windows.Forms.TextBox()
        Me.lblFromDate = New System.Windows.Forms.Label()
        Me.lblDataSpec = New System.Windows.Forms.Label()
        Me.grpRadioBtn.SuspendLayout()
        Me.SuspendLayout()
        '
        'cmdStart
        '
        Me.cmdStart.Location = New System.Drawing.Point(392, 16)
        Me.cmdStart.Name = "cmdStart"
        Me.cmdStart.Size = New System.Drawing.Size(104, 32)
        Me.cmdStart.TabIndex = 5
        Me.cmdStart.Text = "データ取込み開始"
        '
        'lblProgressBar2
        '
        Me.lblProgressBar2.Location = New System.Drawing.Point(16, 152)
        Me.lblProgressBar2.Name = "lblProgressBar2"
        Me.lblProgressBar2.Size = New System.Drawing.Size(136, 16)
        Me.lblProgressBar2.TabIndex = 215
        Me.lblProgressBar2.Text = "読込み"
        '
        'lblProgressBar1
        '
        Me.lblProgressBar1.Location = New System.Drawing.Point(16, 112)
        Me.lblProgressBar1.Name = "lblProgressBar1"
        Me.lblProgressBar1.Size = New System.Drawing.Size(136, 16)
        Me.lblProgressBar1.TabIndex = 214
        Me.lblProgressBar1.Text = "ダウンロード"
        '
        'progressBar2
        '
        Me.progressBar2.Location = New System.Drawing.Point(16, 168)
        Me.progressBar2.Name = "progressBar2"
        Me.progressBar2.Size = New System.Drawing.Size(352, 16)
        Me.progressBar2.TabIndex = 213
        '
        'cmdCancel
        '
        Me.cmdCancel.Location = New System.Drawing.Point(392, 56)
        Me.cmdCancel.Name = "cmdCancel"
        Me.cmdCancel.Size = New System.Drawing.Size(104, 32)
        Me.cmdCancel.TabIndex = 6
        Me.cmdCancel.Text = "データ取込み中止"
        '
        'progressBar1
        '
        Me.progressBar1.Location = New System.Drawing.Point(16, 128)
        Me.progressBar1.Name = "progressBar1"
        Me.progressBar1.Size = New System.Drawing.Size(352, 16)
        Me.progressBar1.TabIndex = 211
        '
        'tmrJVStatus
        '
        Me.tmrJVStatus.Interval = 300
        '
        'grpRadioBtn
        '
        Me.grpRadioBtn.Controls.AddRange(New System.Windows.Forms.Control() {Me.rbtSetup, Me.rbtIsthisweek, Me.rbtNormal})
        Me.grpRadioBtn.Location = New System.Drawing.Point(16, 56)
        Me.grpRadioBtn.Name = "grpRadioBtn"
        Me.grpRadioBtn.Size = New System.Drawing.Size(352, 48)
        Me.grpRadioBtn.TabIndex = 223
        Me.grpRadioBtn.TabStop = False
        Me.grpRadioBtn.Tag = ""
        Me.grpRadioBtn.Text = "取得データ"
        '
        'rbtSetup
        '
        Me.rbtSetup.Location = New System.Drawing.Point(200, 24)
        Me.rbtSetup.Name = "rbtSetup"
        Me.rbtSetup.Size = New System.Drawing.Size(104, 16)
        Me.rbtSetup.TabIndex = 4
        Me.rbtSetup.Tag = ""
        Me.rbtSetup.Text = "セットアップデータ"
        '
        'rbtIsthisweek
        '
        Me.rbtIsthisweek.Location = New System.Drawing.Point(88, 24)
        Me.rbtIsthisweek.Name = "rbtIsthisweek"
        Me.rbtIsthisweek.Size = New System.Drawing.Size(104, 16)
        Me.rbtIsthisweek.TabIndex = 3
        Me.rbtIsthisweek.Tag = ""
        Me.rbtIsthisweek.Text = "今週開催データ"
        '
        'rbtNormal
        '
        Me.rbtNormal.Checked = True
        Me.rbtNormal.Location = New System.Drawing.Point(8, 24)
        Me.rbtNormal.Name = "rbtNormal"
        Me.rbtNormal.Size = New System.Drawing.Size(104, 16)
        Me.rbtNormal.TabIndex = 2
        Me.rbtNormal.TabStop = True
        Me.rbtNormal.Tag = ""
        Me.rbtNormal.Text = "通常データ"
        '
        'txtFromDate
        '
        Me.txtFromDate.Location = New System.Drawing.Point(248, 32)
        Me.txtFromDate.Name = "txtFromDate"
        Me.txtFromDate.Size = New System.Drawing.Size(120, 19)
        Me.txtFromDate.TabIndex = 1
        Me.txtFromDate.Text = ""
        '
        'txtDataSpec
        '
        Me.txtDataSpec.Location = New System.Drawing.Point(16, 32)
        Me.txtDataSpec.Name = "txtDataSpec"
        Me.txtDataSpec.Size = New System.Drawing.Size(224, 19)
        Me.txtDataSpec.TabIndex = 0
        Me.txtDataSpec.Text = ""
        '
        'lblFromDate
        '
        Me.lblFromDate.Location = New System.Drawing.Point(248, 16)
        Me.lblFromDate.Name = "lblFromDate"
        Me.lblFromDate.Size = New System.Drawing.Size(120, 16)
        Me.lblFromDate.TabIndex = 222
        Me.lblFromDate.Text = "データ提供日FROM"
        '
        'lblDataSpec
        '
        Me.lblDataSpec.Location = New System.Drawing.Point(16, 16)
        Me.lblDataSpec.Name = "lblDataSpec"
        Me.lblDataSpec.Size = New System.Drawing.Size(160, 16)
        Me.lblDataSpec.TabIndex = 221
        Me.lblDataSpec.Text = "ファイル識別子"
        '
        'frmJVLinkDialog
        '
        Me.AutoScaleBaseSize = New System.Drawing.Size(5, 12)
        Me.ClientSize = New System.Drawing.Size(506, 198)
        Me.Controls.AddRange(New System.Windows.Forms.Control() {Me.grpRadioBtn, Me.txtFromDate, Me.txtDataSpec, Me.lblFromDate, Me.lblDataSpec, Me.cmdStart, Me.lblProgressBar2, Me.lblProgressBar1, Me.progressBar2, Me.cmdCancel, Me.progressBar1})
        Me.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow
        Me.MaximizeBox = False
        Me.MinimizeBox = False
        Me.Name = "frmJVLinkDialog"
        Me.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen
        Me.Text = "Sample1 JVLink Dialog"
        Me.grpRadioBtn.ResumeLayout(False)
        Me.ResumeLayout(False)

    End Sub

#End Region

    Private frmOwner As frmMain                 ''メインフォーム
    Private CancelFlag As Boolean               ''キャンセルフラグ
    Private ReadCount As Integer                ''JVOpen:総読込みファイル数
    Private DownloadCount As Integer            ''JVOpen:総ダウンロードファイル数
    Private LastFileTimeStamp As String         ''JVOpen:最後にダウンロードしたファイルのタイムスタンプ


    '------------------------------------------------------------------------------------------------
    '　　データ取得実行ボタンクリック時の処理
    '------------------------------------------------------------------------------------------------
    Private Sub cmdStart_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                                Handles cmdStart.Click
        Try

            Dim DataSpec As String              ''引数 JVOpen:ファイル識別子
            Dim FromDate As String              ''引数 JVOpen:データ提供日付FROM
            Dim DataOption As Integer           ''引数 JVOpen:オプション
            Dim ReturnCode As Integer           ''JVLink戻値

            '初期値設定
            tmrJVStatus.Enabled = False         ''タイマー停止
            frmOwner = Owner                    ''親フォームを指定
            CancelFlag = False                  ''キャンセルフラグ初期化
            progressBar1.Value = 0              ''プログレスバー初期化
            progressBar2.Value = 0

            '引数設定
            DataSpec = txtDataSpec.Text
            FromDate = txtFromDate.Text

            If rbtNormal.Checked = True Then
                DataOption = 1
            ElseIf rbtIsthisweek.Checked = True Then
                DataOption = 2
            ElseIf rbtSetup.Checked = True Then
                DataOption = 3
            End If

            Cursor = Cursors.AppStarting()

            '**********************
            'JVLinkダウンロード処理
            '**********************
            ReturnCode = frmOwner.AxJVLink1.JVOpen(DataSpec, _
                                             FromDate, _
                                             DataOption, _
                                             ReadCount, _
                                             DownloadCount, _
                                             LastFileTimeStamp)

            'エラー判定
            If ReturnCode <> 0 Then     ''エラー
                Call frmOwner.PrintOut("JVOpenエラー:" & ReturnCode & ControlChars.CrLf)
                '終了処理
                Call JVClosing()
            Else                        ''正常
                Call frmOwner.PrintOut("JVOpen正常終了:" & ReturnCode & ControlChars.CrLf)
                Call frmOwner.PrintOut("ReadCount:" & _
                                        ReadCount & _
                                        " , DownloadCount:" & _
                                        DownloadCount & _
                                        ControlChars.CrLf)

                '総ダウンロード数をチェック
                If DownloadCount = 0 Then                       ''総ダウンロード数=０
                    'プログレスバー100％表示
                    progressBar1.Maximum = 100                  ''MAXを100に設定
                    progressBar1.Value = progressBar1.Maximum   ''プログレスバー100％表示
                    Text = "ダウンロード完了"
                    '読込み処理
                    Call JVReading()
                    '終了処理
                    Call JVClosing()
                Else                                            ''総ダウンロード数が０以上
                    '初期値設定
                    Text = "ダウンロード中・・・"
                    progressBar1.Maximum = DownloadCount        ''プログレスバーのＭＡＸ値設定
                    'タイマー始動：ダウンロード進捗率をプログレスバー表示
                    tmrJVStatus.Enabled = True                  ''ダウンロードステータスを監視する
                End If
            End If

            '終了
            Exit Sub

        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub


    '------------------------------------------------------------------------------------------------
    '　　タイマー：ダウンロード状況をプログレスバー表示
    '------------------------------------------------------------------------------------------------
    Private Sub tmrJVStatus_Tick(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                            Handles tmrJVStatus.Tick
        Try
            Dim ReturnCode As Integer           ''JVLink戻値

            '**********************
            'JVLinkダウンロード進捗率
            '**********************
            ReturnCode = frmOwner.AxJVLink1.JVStatus                ''ダウンロード済のファイル数を返す

            'エラー判定
            If ReturnCode < 0 Then                                  ''エラー
                Call frmOwner.PrintOut("JVStatusエラー:" & ReturnCode)
                'タイマー停止
                tmrJVStatus.Enabled = False
                '終了処理
                Call JVClosing()
                '終了
                Exit Sub
            ElseIf ReturnCode < DownloadCount Then                  ''ステータス
                'プログレス表示
                Text = "ダウンロード中．．．(" & ReturnCode & "/" & DownloadCount & ")"
                progressBar1.Value = ReturnCode
            ElseIf ReturnCode = DownloadCount Then                  ''ステータス100％
                'タイマー停止
                tmrJVStatus.Enabled = False
                'プログレス表示
                Text = "ダウンロード完了(" & ReturnCode & "/" & DownloadCount & ")"
                progressBar1.Value = ReturnCode
                '読込み処理
                Call JVReading()
                '終了処理
                Call JVClosing()
                '終了
                Exit Sub
            End If

        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　読込処理 
    '------------------------------------------------------------------------------------------------
    Public Sub JVReading()
        Try
            Dim Buff As String                          ''バッファ
            Dim BuffSize As Integer                     ''バッファサイズ
            Dim BuffName As String                      ''バッファ名
            Dim JVReadingCount As Integer               ''読込みファイル数
            Dim ReturnCode As Integer                   ''JVLink戻値
            Dim bytData(0) As Byte                      ''JVGets用バッファポインタ

            '初期値設定
            progressBar2.Maximum = ReadCount
            JVReadingCount = 0
            progressBar2.Value = 0
            Text = "データ読込み中．．．(0/" & ReadCount & ")"

            'バッファ領域確保
            BuffSize = 110000
            Buff = New String(vbNullChar, BuffSize)
            BuffName = String.Empty

            Do
                'バックグラウンドでの処理
                System.Windows.Forms.Application.DoEvents()

                'キャンセルが押されたら処理を抜ける
                If CancelFlag = True Then Exit Sub

                '**********************
                'JVLink読込み処理
                '**********************
                'ReturnCode = frmOwner.AxJVLink1.JVRead(Buff, BuffSize, BuffName)
#Disable Warning BC41999
                ReturnCode = frmOwner.AxJVLink1.JVGets(bytData, BuffSize, BuffName)
#Enable Warning BC41999
                'エラー判定
                Select Case ReturnCode
                    Case Is > 0      ''正常

                        ' JVReadが正常に終了した場合はバッファーの内容を画面に表示します。
                        ' サンプルプログラムであるため単純に全てのデータを表示していますが、画面表示
                        ' は時間のかかる処理であるため読み込み処理全体の実行時間が遅くなっています。
                        ' 必要に応じて下の１行をコメントアウトするか他の処理に置き換えてください。
                        'Call frmOwner.PrintOut(Buff)
                        Call frmOwner.PrintOut(System.Text.Encoding.GetEncoding(932).GetString(bytData))
						ReDim bytData(0)

                    Case -1          ''ファイルの切れ目
                        'ファイル名表示
                        Call frmOwner.PrintFilelist(BuffName & ControlChars.CrLf)
                        Call frmOwner.PrintOut("JVRead File :" & ReturnCode & ControlChars.CrLf)
                        'プログレスバー表示
                        JVReadingCount = JVReadingCount + 1 ''カウントアップ
                        progressBar2.Value = JVReadingCount
                        Text = "データ読込み中．．．(" & JVReadingCount & "/" & ReadCount & ")"
                    Case 0          ''全レコード読込み終了(EOF)
                        Call frmOwner.PrintOut("JVRead EndOfFile :" & ReturnCode & ControlChars.CrLf)
                        Text = "データ読込み完了(" & JVReadingCount & "/" & ReadCount & ")"
                        '終了
                        Exit Sub
                    Case Is < -1     ''エラー
                        Call frmOwner.PrintOut("JVReadエラー:" & ReturnCode & ControlChars.CrLf)
                        '終了
                        Exit Sub
                End Select

            Loop
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　キャンセルボタンクリック時の処理
    '------------------------------------------------------------------------------------------------
    Private Sub cmdCancel_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                            Handles cmdCancel.Click
        Try
            'タイマー停止
            tmrJVStatus.Enabled = False

            '***************
            'JVLink中止処理
            '***************
            frmOwner.AxJVLink1.JVCancel()

            'キャンセルフラグをたてる
            CancelFlag = True

            Call frmOwner.PrintOut("JVCancel:キャンセルされました" & ControlChars.CrLf)
            Text = "JVCancel:キャンセルされました"

            '終了
            Exit Sub
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　終了処理
    '------------------------------------------------------------------------------------------------
    Private Sub JVClosing()
        Try
            Dim ReturnCode As Integer           ''JVLink戻値

            '***************
            'JVLink終了処理
            '***************
            ReturnCode = frmOwner.AxJVLink1.JVClose()

            Cursor = Cursors.Default()

            If ReturnCode <> 0 Then         ''エラー
                Call frmOwner.PrintOut("JVCloseエラー:" & CStr(ReturnCode) & ControlChars.CrLf)
            Else                            ''正常
                Call frmOwner.PrintOut("JVClose正常終了:" & CStr(ReturnCode) & ControlChars.CrLf)
            End If

            '終了
            Exit Sub
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

End Class
