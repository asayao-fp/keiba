'========================================================================
'  JRA-VAN Data Lab. サンプルプログラム１(Form1.vb)
'
'
'   作成: JRA-VAN ソフトウェア工房  2003年4月22日
'
'========================================================================
'   (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
'========================================================================

Public Class frmMain
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
    Friend WithEvents lblFileList As System.Windows.Forms.Label
    Friend WithEvents lblPrint As System.Windows.Forms.Label
    Friend WithEvents cmdJVSetUIProperties As System.Windows.Forms.Button
    Friend WithEvents cmdDelete As System.Windows.Forms.Button
    Friend WithEvents cmdJVLinkDialog As System.Windows.Forms.Button
    Friend WithEvents txtOut As System.Windows.Forms.RichTextBox
    Friend WithEvents txtFileList As System.Windows.Forms.RichTextBox
    Friend WithEvents cmdClear As System.Windows.Forms.Button
    Friend WithEvents AxJVLink1 As AxJVDTLabLib.AxJVLink

    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        Dim resources As System.Resources.ResourceManager = New System.Resources.ResourceManager(GetType(frmMain))
        Me.lblFileList = New System.Windows.Forms.Label()
        Me.lblPrint = New System.Windows.Forms.Label()
        Me.cmdJVLinkDialog = New System.Windows.Forms.Button()
        Me.cmdJVSetUIProperties = New System.Windows.Forms.Button()
        Me.cmdDelete = New System.Windows.Forms.Button()
        Me.txtOut = New System.Windows.Forms.RichTextBox()
        Me.txtFileList = New System.Windows.Forms.RichTextBox()
        Me.cmdClear = New System.Windows.Forms.Button()
        Me.AxJVLink1 = New AxJVDTLabLib.AxJVLink()
        CType(Me.AxJVLink1, System.ComponentModel.ISupportInitialize).BeginInit()
        Me.SuspendLayout()
        '
        'lblFileList
        '
        Me.lblFileList.Location = New System.Drawing.Point(400, 96)
        Me.lblFileList.Name = "lblFileList"
        Me.lblFileList.Size = New System.Drawing.Size(128, 16)
        Me.lblFileList.TabIndex = 200
        Me.lblFileList.Text = "読込みファイルリスト"
        '
        'lblPrint
        '
        Me.lblPrint.Location = New System.Drawing.Point(16, 96)
        Me.lblPrint.Name = "lblPrint"
        Me.lblPrint.Size = New System.Drawing.Size(56, 16)
        Me.lblPrint.TabIndex = 198
        Me.lblPrint.Text = "出力"
        '
        'cmdJVLinkDialog
        '
        Me.cmdJVLinkDialog.Location = New System.Drawing.Point(16, 16)
        Me.cmdJVLinkDialog.Name = "cmdJVLinkDialog"
        Me.cmdJVLinkDialog.Size = New System.Drawing.Size(88, 32)
        Me.cmdJVLinkDialog.TabIndex = 0
        Me.cmdJVLinkDialog.Text = "データ取込み"
        '
        'cmdJVSetUIProperties
        '
        Me.cmdJVSetUIProperties.Location = New System.Drawing.Point(112, 16)
        Me.cmdJVSetUIProperties.Name = "cmdJVSetUIProperties"
        Me.cmdJVSetUIProperties.Size = New System.Drawing.Size(88, 32)
        Me.cmdJVSetUIProperties.TabIndex = 1
        Me.cmdJVSetUIProperties.Text = "JVLink設定"
        '
        'cmdDelete
        '
        Me.cmdDelete.Location = New System.Drawing.Point(208, 16)
        Me.cmdDelete.Name = "cmdDelete"
        Me.cmdDelete.Size = New System.Drawing.Size(88, 32)
        Me.cmdDelete.TabIndex = 2
        Me.cmdDelete.Text = "ファイル削除"
        '
        'txtOut
        '
        Me.txtOut.Font = New System.Drawing.Font("ＭＳ ゴシック", 9.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(128, Byte))
        Me.txtOut.Location = New System.Drawing.Point(16, 112)
        Me.txtOut.Name = "txtOut"
        Me.txtOut.RightMargin = 34000
        Me.txtOut.Size = New System.Drawing.Size(376, 336)
        Me.txtOut.TabIndex = 4
        Me.txtOut.Text = ""
        Me.txtOut.WordWrap = False
        '
        'txtFileList
        '
        Me.txtFileList.Font = New System.Drawing.Font("ＭＳ ゴシック", 9.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(128, Byte))
        Me.txtFileList.Location = New System.Drawing.Point(400, 112)
        Me.txtFileList.Name = "txtFileList"
        Me.txtFileList.Size = New System.Drawing.Size(208, 336)
        Me.txtFileList.TabIndex = 5
        Me.txtFileList.Text = ""
        Me.txtFileList.WordWrap = False
        '
        'cmdClear
        '
        Me.cmdClear.Location = New System.Drawing.Point(304, 16)
        Me.cmdClear.Name = "cmdClear"
        Me.cmdClear.Size = New System.Drawing.Size(88, 32)
        Me.cmdClear.TabIndex = 3
        Me.cmdClear.Text = "テキストクリア"
        '
        'AxJVLink1
        '
        Me.AxJVLink1.Enabled = True
        Me.AxJVLink1.Location = New System.Drawing.Point(448, 16)
        Me.AxJVLink1.Name = "AxJVLink1"
        Me.AxJVLink1.OcxState = CType(resources.GetObject("AxJVLink1.OcxState"), System.Windows.Forms.AxHost.State)
        Me.AxJVLink1.Size = New System.Drawing.Size(72, 40)
        Me.AxJVLink1.TabIndex = 201
        '
        'frmMain
        '
        Me.AutoScaleBaseSize = New System.Drawing.Size(5, 12)
        Me.ClientSize = New System.Drawing.Size(626, 464)
        Me.Controls.AddRange(New System.Windows.Forms.Control() {Me.AxJVLink1, Me.cmdClear, Me.txtFileList, Me.txtOut, Me.cmdDelete, Me.cmdJVSetUIProperties, Me.cmdJVLinkDialog, Me.lblFileList, Me.lblPrint})
        Me.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow
        Me.Name = "frmMain"
        Me.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen
        Me.Text = "Sample1 Main Form"
        CType(Me.AxJVLink1, System.ComponentModel.ISupportInitialize).EndInit()
        Me.ResumeLayout(False)

    End Sub

#End Region
    '------------------------------------------------------------------------------------------------
    '　　初期化
    '------------------------------------------------------------------------------------------------
    Private Sub frmMain_Load(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                                Handles MyBase.Load
        Try
            Dim sid As String                   ''引数 JVInit:ソフトウェアID
            Dim ReturnCode As String            ''戻値

            '引数設定
            sid = "UNKNOWN"
            '***************
            'JVLink初期化
            '***************
            '※※※ JVInitは JVLinkメソッド使用前(但し、JVSetUIProPertiesを除く)に呼出す
            ReturnCode = AxJVLink1.JVInit(sid)

            'エラー判定
            If ReturnCode <> 0 Then     ''エラー
                Call PrintOut("JVInitエラー:" & ReturnCode & ControlChars.CrLf)
                '終了
                Exit Sub
            Else                        ''正常
                Call PrintOut("JVInit正常終了:" & ReturnCode & ControlChars.CrLf)
            End If
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　データ取込みボタンクリック時の処理
    '------------------------------------------------------------------------------------------------
    Private Sub cmdJVLinkDialog_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                        Handles cmdJVLinkDialog.Click
        Try
            'Form2：JVLinkコントロールパネルを開く
            Dim frmDialog As frmJVLinkDialog
            frmDialog = New frmJVLinkDialog()
            frmDialog.ShowDialog(Me)            ''オーナーウィンドウに自画面を指定

            frmDialog.Dispose()
            frmDialog = Nothing

            '終了
            Exit Sub
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　「出力」に処理結果を表示
    '------------------------------------------------------------------------------------------------
    Public Sub PrintOut(ByVal Message As String)
        Try
            'txtOutに文字列を表示
            txtOut.AppendText(Message)
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub


    '------------------------------------------------------------------------------------------------
    '　　「ファイルリスト」にダウンロードしたファイルリストを表示
    '------------------------------------------------------------------------------------------------
    Public Sub PrintFilelist(ByVal strMessage As String)
        Try
            'txtFileListに文字列を表示
            txtFileList.AppendText(strMessage)
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　JVLink設定ウィンドウ表示
    '------------------------------------------------------------------------------------------------
    Private Sub cmdJVSetUIProperties_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                    Handles cmdJVSetUIProperties.Click
        Try
            Dim ReturnCode As String            ''戻値

            '**********************
            'JVLink設定画面表示
            '**********************
            ReturnCode = AxJVLink1.JVSetUIProperties()

            'エラー判定
            If ReturnCode <> 0 Then         ''エラー
                PrintOut("JVSetUIPropertiesエラー:" & ReturnCode & ControlChars.CrLf)
            Else                            ''正常
                PrintOut("JVSetUIProperties正常終了:" & ReturnCode & ControlChars.CrLf)
            End If

            '終了
            Exit Sub
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　指定したファイルを削除
    '------------------------------------------------------------------------------------------------
    Private Sub cmdDelete_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                            Handles cmdDelete.Click
        Try
            Dim Message, Title, DefaultValue As String
            Dim MyValue As String
            Dim ReturnCode As String            ''戻値

            '初期値設定
            Message = "ファイル名を入力して下さい"                          ''メッセージ
            Title = "ファイル削除"                                          ''タイトル名
            DefaultValue = ""                                               ''初期値

            'ファイル名入力
            MyValue = InputBox(Message, Title, DefaultValue)

            '**********************
            'JVFileDelete
            '**********************
            ReturnCode = AxJVLink1.JVFiledelete(MyValue)

            'エラー判定
            If ReturnCode <> 0 Then         ''エラー
                PrintOut("JVFiledeleteエラー:" & ReturnCode & ControlChars.CrLf)
            Else                            ''正常
                PrintOut("JVFiledelete正常終了:" & ReturnCode & ControlChars.CrLf)
            End If

            '終了
            Exit Sub
        Catch
            Debug.WriteLine(Err.Description)
        End Try
    End Sub

    '------------------------------------------------------------------------------------------------
    '　　表示をクリア
    '------------------------------------------------------------------------------------------------
    Private Sub cmdClear_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) _
                                                                                Handles cmdClear.Click
        txtOut.Text = ""
        txtFileList.Text = ""
    End Sub

End Class
