//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(Unit1)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

unit Unit1;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, StdCtrls, OleCtrls, JVDTLabLib_TLB, ComCtrls;

type
  TfrmMain = class(TForm)
	Label5: TLabel;
	Label6: TLabel;
	ButtonJVLinkDialog: TButton;
	ButtonJVSetUIProperties: TButton;
	ButtonDelete: TButton;
	JVLink1: TJVLink;
	ButtonClear: TButton;
	txtOut: TRichEdit;
	txtFilelist: TRichEdit;
	procedure ButtonJVLinkDialogClick(Sender: TObject);
	procedure ButtonJVSetUIPropertiesClick(Sender: TObject);
	procedure ButtonDeleteClick(Sender: TObject);
	procedure ButtonClearClick(Sender: TObject);
	procedure FormShow(Sender: TObject);

  private
	{ Private 宣言 }

  public
	{ Public 宣言 }
	procedure PrintOut(strMessage : WideString);
	procedure PrintFilelist(strMessage : WideString);
  end;

var
   frmMain: TfrmMain;

implementation

uses Unit2;

{$R *.dfm}

//------------------------------------------------------------------------------
//		初期化
//------------------------------------------------------------------------------
procedure TfrmMain.FormShow(Sender: TObject);
var
	sid : WideString;					//引数 JVInit:ソフトウェアID
	ReturnCode: Integer;
begin
	//引数設定
	sid := 'UNKNOWN';

	//**********************
	//JVLink初期化
	//**********************
	//※※※ JVInitは JVLinkメソッド使用前（但し、JVSetUIProPertiesを除く）に呼出す
	ReturnCode := JVLink1.JVInit(sid);

	//エラー判定
	If ReturnCode <> 0 Then begin		//エラー
		frmMain.PrintOut('JVInitエラー:' + IntToStr(ReturnCode) + #13#10 );
		Exit;
		end
	else								//正常
		frmMain.PrintOut('JVInit正常終了:' + intToStr(ReturnCode) + #13#10 );
end;


//------------------------------------------------------------------------------
//		データ取込みボタンクリック時の処理
//------------------------------------------------------------------------------
procedure TfrmMain.ButtonJVLinkDialogClick(Sender: TObject);
var
	frmJVLinkDialog:TfrmJVLinkDialog;
begin
	frmJVLinkDialog := TfrmJVLinkDialog.Create(Self);
	//Form2:JVLinkダイアログを開く
	frmJVLinkDialog.Showmodal;
	frmJVLinkDialog.Free;
	Exit;
end;

//------------------------------------------------------------------------------
//		指定したファイルを削除
//------------------------------------------------------------------------------
procedure TfrmMain.ButtonDeleteClick(Sender: TObject);
var
	MessageStr: WideString;
	Title: WideString;
	DefaultValue: WideString;
	MyValue: String;
	ReturnCode: Integer;
begin

	MessageStr := 'ファイル名を入力して下さい'; 			 //メッセージ
	Title := 'ファイル削除';								 //タイトル名
	DefaultValue := ''; 									 //初期値

	MyValue := InputBox(MessageStr, Title, DefaultValue);

	//**********************
	//JVFileDelete
	//**********************
	ReturnCode := JVLink1.JVFiledelete(MyValue);
	If ReturnCode <> 0 Then
		frmMain.PrintOut('JVFiledeleteエラー:' + IntToStr(Returncode) + #13#10 )
	Else
		frmMain.PrintOut('JVFiledelete正常終了:' + IntToStr(Returncode) + #13#10 );

end;

//------------------------------------------------------------------------------
//　　JVLink設定ウィンドウ表示
//------------------------------------------------------------------------------
procedure TfrmMain.ButtonJVSetUIPropertiesClick(Sender: TObject);
var
	ReturnCode: Integer;
begin
	//**********************
	//JVLink設定画面表示
	//**********************
	ReturnCode:=JVLink1.JVSetUIProperties();
	If ReturnCode <> 0 Then
		frmMain.PrintOut('JVSetUIPropertiesエラー:' + IntToStr(Returncode) + #13#10 )
	Else
		frmMain.PrintOut('JVSetUIProperties正常終了:' + IntToStr(Returncode) + #13#10 );

end;

//------------------------------------------------------------------------------
//		「出力」に処理結果を表示
//------------------------------------------------------------------------------
procedure TfrmMain.PrintOut(strMessage : WideString);
begin
	//txtOutに文字列を表示し、改行する
	txtOut.SelStart:= txtOut.GetTextLen;
	txtOut.SelText:=strMessage;
	txtOut.Perform(EM_SCROLL, SB_PAGEDOWN, 0);
	Exit;
end;

//------------------------------------------------------------------------------
//		「ファイルリスト」にダウンロードしたファイルリストを表示
//------------------------------------------------------------------------------
procedure TfrmMain.PrintFilelist(strMessage : WideString);
begin
	//txtFileListに文字列を表示し、改行する
	txtFilelist.SelStart:= txtOut.GetTextLen;
	txtFilelist.SelText:=strMessage;
	txtFilelist.Perform(EM_SCROLL, SB_LINEDOWN, 0);

	Exit;
end;

//------------------------------------------------------------------------------
//		表示をクリア
//------------------------------------------------------------------------------
procedure TfrmMain.ButtonClearClick(Sender: TObject);
begin
	txtOut.Text:='';
	txtFilelist.Text:='';
end;


end.


