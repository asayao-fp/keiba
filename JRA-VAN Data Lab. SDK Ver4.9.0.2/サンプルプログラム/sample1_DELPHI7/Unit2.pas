//========================================================================
//  JRA-VAN Data Lab. サンプルプログラム１(Unit2)
//
//
//   作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//   (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================
unit Unit2;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, ComCtrls, StdCtrls, ExtCtrls, OleCtrls, JVDTLabLib_TLB ;

type
    TfrmJVLinkDialog = class(TForm)
    ButtonCancel: TButton;
    ProgressBar1: TProgressBar;
    ProgressBar2: TProgressBar;
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    txtDataSpec: TEdit;
    txtFromDate: TEdit;
    ButtonStart: TButton;
    GroupBox1: TGroupBox;
    rbtNormal: TRadioButton;
    rbtIsthisweek: TRadioButton;
    rbtSetup: TRadioButton;
    TimerJVStatus: TTimer;

    procedure ButtonCancelClick(Sender: TObject);
    procedure ButtonStartClick(Sender: TObject);
    procedure TimerJVStatusTimer(Sender: TObject);
    procedure FormShow(Sender: TObject);

  private
    { Private 宣言 }
    procedure JVClosing();
    procedure JVReading();
  public
    { Public 宣言 }

  end;

var
    frmJVLinkDialog: TfrmJVLinkDialog;
    DialogCancel : Boolean;             //キャンセルフラグ
    ReadCount : Integer;                //JVOpen:総読込みファイル数
    DownloadCount : Integer;            //JVOpen:総ダウンロードファイル数
    LastFileTimeStamp : WideString;     //JVOpen:最後にダウンロードしたファイルのタイムスタンプ

implementation

uses Unit1;

{$R *.dfm}

//------------------------------------------------------------------------------
//      初期処理
//------------------------------------------------------------------------------
procedure TfrmJVLinkDialog.FormShow(Sender: TObject);
begin
    // ウィンドウを常に最上位に表示する
    SetWindowPos(Handle, HWND_TOPMOST, 0, 0, 0, 0,
    SWP_NOMOVE or SWP_NOSIZE or SWP_NOACTIVATE);
end;

//------------------------------------------------------------------------------
//      データ取得実行ボタンクリック時の処理
//------------------------------------------------------------------------------
procedure TfrmJVLinkDialog.ButtonStartClick(Sender: TObject);
var
    DataSpec : WideString;
    FromDate : WideString;
    DataOption : Integer;
    ReturnCode  : Integer;              //JVLink返値
begin
    TimerJVStatus.Enabled:=false;       //タイマー停止
    DialogCancel:=false;                //キャンセルフラグ初期化
    ProgressBar1.Position:=0;           //プログレスバー初期化
    ProgressBar2.Position:=0;
    //引数設定

    DataSpec := txtDataSpec.Text;       //引数 ファイル識別子
    FromDate := txtFromDate.text;       //引数 データ提供日付FROM

    if rbtNormal.Checked=true then
        DataOption:=1
    else if rbtIsthisweek.Checked=true then
        DataOption:=2
    else if rbtSetup.Checked=true then
        DataOption:=3;

    Cursor:=crAppStart;

    //**********************
    //JVLinkダウンロード処理
    //**********************
    ReturnCode := frmMain.JVLink1.JVOpen(DataSpec,
                                         FromDate,
                                         DataOption,
                                         ReadCount,
                                         DownloadCount,
                                         LastFileTimeStamp);

        //エラー判定
    If ReturnCode <> 0 Then begin       //エラー
        frmMain.PrintOut('JVOpenエラー:' + IntToStr(ReturnCode) + #13#10 );
        //終了処理
        JVClosing;
        end
    Else begin                          //正常
        frmMain.PrintOut('JVOpen正常終了:' + IntToStr(ReturnCode) + #13#10 );
        frmMain.PrintOut('ReadCount:' +
                           IntToStr(ReadCount) +
                           ', DownloadCount:' +
                           IntToStr(DownloadCount) +
                           #13#10 );
        //総ダウンロード数チェック
        If DownloadCount=0 then begin         //総ダウンロード数＝０
            //プログレスバー１００％表示
            ProgressBar1.max:=100;                       //MAXを１００に設定
            ProgressBar1.Position:=ProgressBar1.Max;     //プログレスバーを１００％表示
            //読込み処理
            JVReading();
            //終了処理
            JVClosing();
            end
        else begin                           //総ダウンロード数が０以上
            //初期設定
            Caption:='ダウンロード中・・・';
            ProgressBar1.Max:=DownloadCount;            //プログレスバーのMAX値設定
            TimerJVStatus.Enabled := true;              //タイマー起動
        end;
    end;

    //終了
    Exit;
end;

//------------------------------------------------------------------------------
//      タイマー：ダウンロード進捗率をプログレスバー表示
//------------------------------------------------------------------------------
procedure TfrmJVLinkDialog.TimerJVStatusTimer(Sender: TObject);
var
    ReturnCode  : Integer;              //JVLink返値
begin
    //**********************
    //JVLinkダウンロード進捗率
    //**********************
        ReturnCode :=  frmMain.JVLink1.JVStatus;

    //エラー判定
    If ReturnCode < 0 Then begin
        frmMain.PrintOut('JVStatusエラー:' + IntToStr(ReturnCode) + #13#10 );
        //タイマー停止
        TimerJVStatus.Enabled:=false;
        //終了処理
        JVClosing;
        //終了
        Exit;
        end
    Else If ReturnCode<DownloadCount then begin
        //プログレスバー表示
        Caption :='ダウンロード中．．．('+ IntToStr(ReturnCode) + '/' + IntToStr(DownloadCount) + ')';
        ProgressBar1.Position:=ReturnCode;
        end
    Else If ReturnCode=DownloadCount then begin
        //タイマー停止
        TimerJVStatus.Enabled:=false;
        //プログレスバー表示
        Caption :='ダウンロード中．．．('+ IntToStr(ReturnCode) + '/' + IntToStr(DownloadCount) + ')';
        ProgressBar1.Position:= ReturnCode;
        //読込み処理
        JVReading;
        //終了処理
        JVClosing;
        //終了
        Exit;
    end;
end;


//------------------------------------------------------------------------------
//      読込み処理
//------------------------------------------------------------------------------
procedure TfrmJVLinkDialog.JVReading();
var
    BuffAnsi : AnsiString;              //バッファ
    BuffSize : Integer;             //バッファサイズ
    BuffName : WideString;          //バッファ名
    JVReadingCount : Integer;       //読込みファイル数
    ReturnCode  : Integer;              //JVLink返値
    BuffVar : OleVariant;
    P: Pointer;
begin
    //初期値
    JVReadingCount:=0;
    Caption := 'データ読込み中．．．(0/' + IntToStr(ReadCount) + ')';
    ProgressBar2.Position := 0;
    ProgressBar2.Max:=ReadCount;
    //バッファ領域確保
    BuffSize := 110000;

    while True do begin


        //バックグラウンドでの処理
        Application.ProcessMessages;
    	BuffVar := VarArrayCreate([0, 0],varByte);

        //キャンセルが押されたら処理を抜ける
        if DialogCancel=true then exit;

            //***************
            //JVLink読込み処理
            //***************
//            ReturnCode :=  frmMain.JVLink1.JVRead(Buff, BuffSize, BuffName);
            ReturnCode :=  frmMain.JVLink1.JVGets(BuffVar, BuffSize, BuffName);
            //エラー判定
            if ReturnCode > 0 then begin          //正常終了

                // JVReadが正常に終了した場合はバッファーの内容を画面に表示します。
                // サンプルプログラムであるため単純に全てのデータを表示していますが、画面表示
                // は時間のかかる処理であるため読み込み処理全体の実行時間が遅くなっています。
                // 必要に応じて下の１行をコメントアウトするか他の処理に置き換えてください。

                SetLength(BuffAnsi, ReturnCode);
                P := VarArrayLock(BuffVar);
                try
                   Move(P^, BuffAnsi[1], ReturnCode);
                finally
                   VarArrayUnlock(BuffVar);
                   VarClear(BuffVar);
               end;
                frmMain.PrintOut(BuffAnsi);

                end
            else if ReturnCode = -1 then begin    //ファイルの切れ目
                //ファイル名表示
                frmMain.PrintFilelist(BuffName + #13#10 );
                frmMain.PrintOut('Read File:'+ IntToStr(ReturnCode) + #13#10 );
                //プログレスバー表示
                JVReadingCount:=JVReadingCount+1; //カウントアップ
                ProgressBar2.Position:=JVReadingCount;
                Caption := 'データ読込み中．．．(' + IntToStr(JVReadingCount) + '/' + IntToStr(ReadCount) + ')';
                end

            else if ReturnCode = 0 then begin     //全レコード読込み終了(EOF)
                frmMain.PrintOut('JVRead EndOfFile :' + IntToStr(ReturnCode) + #13#10 );
                Caption := 'データ読込み完了(' + IntToStr(JVReadingCount) + '/' + IntToStr(ReadCount) + ')';
                //Repeatを抜ける
                Break;
                end
            else if ReturnCode < -1 then begin    //読込みエラー
                frmMain.PrintOut('JVReadエラー:' + IntToStr(ReturnCode) + #13#10 );
                //Repeatを抜ける
                Break;
                end;

    end;

    Exit;

end;

//------------------------------------------------------------------------------
//      キャンセルボタンクリック時の処理
//------------------------------------------------------------------------------
procedure TfrmJVLinkDialog.ButtonCancelClick(Sender: TObject);
begin
    //タイマーを終了する
    TimerJVStatus.Enabled := False;

    //***************
    //JVLink中止処理
    //***************
     frmMain.JVLink1.JVCancel();

    //キャンセルフラグをたてる
    DialogCancel:=true;

    //メッセージ表示
    frmMain.PrintOut('JVCancel:キャンセルされました' + #13#10 );
    Caption := 'JVCancel:キャンセルされました';

    Exit;
end;

//------------------------------------------------------------------------------
//      終了処理
//------------------------------------------------------------------------------
procedure TfrmJVLinkDialog.JVClosing();
var
    ReturnCode  : Integer;              //JVLink返値
begin

    //***************
    //JVLink終了処理
    //***************
    ReturnCode :=  frmMain.JVLink1.JVClose;

    Cursor:=crDefault;

    //エラー判定
    If ReturnCode <> 0 Then     //エラー
        frmMain.PrintOut('JVCloseエラー:' + IntToStr(ReturnCode) + #13#10 )
    Else                        //正常
        frmMain.PrintOut('JVClose正常終了:' + IntToStr(ReturnCode) + #13#10 );

    Exit;
end;

end.
