//========================================================================
//  JRA-VAN Data Lab. サンプルプログラム１(JVLinkSample1)
//
//
//   作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//   (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================
program JVLinkSample1;

uses
  Forms,
  Unit1 in 'Unit1.pas' {frmMain},
  Unit2 in 'Unit2.pas' {frmJVLinkDialog};

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TfrmMain, frmMain);
  Application.CreateForm(TfrmJVLinkDialog, frmJVLinkDialog);
  Application.Run;
end.
