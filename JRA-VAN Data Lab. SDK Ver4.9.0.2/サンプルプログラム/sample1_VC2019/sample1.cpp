//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(sample1.cpp)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

#include "stdafx.h"
#include "sample1.h"
#include "sample1Dlg1.h"
#include "JVLink.h"
#include "jvlink.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CSample1App

BEGIN_MESSAGE_MAP(CSample1App, CWinApp)
	//{{AFX_MSG_MAP(CSample1App)
		// メモ - ClassWizard はこの位置にマッピング用のマクロを追加または削除します。
		//        この位置に生成されるコードを編集しないでください。
	//}}AFX_MSG
	ON_COMMAND(ID_HELP, CWinApp::OnHelp)
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CSample1App クラスの構築

CSample1App::CSample1App()
{
	// TODO: この位置に構築用のコードを追加してください。
	// ここに InitInstance 中の重要な初期化処理をすべて記述してください。
}

/////////////////////////////////////////////////////////////////////////////
// 唯一の CSample1App オブジェクト

CSample1App theApp;

/////////////////////////////////////////////////////////////////////////////
// CSample1App クラスの初期化

BOOL CSample1App::InitInstance()
{
	AfxEnableControlContainer();

	//リッチエディット初期化
	AfxInitRichEdit();

	// 標準的な初期化処理
	// もしこれらの機能を使用せず、実行ファイルのサイズを小さくしたけ
	//  れば以下の特定の初期化ルーチンの中から不必要なものを削除して
	//  ください。


#if _MSC_VER <= 1200
	#ifdef _AFXDLL
		Enable3dControls();			// 共有 DLL 内で MFC を使う場合はここをコールしてください。
	#else
		Enable3dControlsStatic();	// MFC と静的にリンクする場合はここをコールしてください。
	#endif
#endif

	CSample1Dlg1 dlg;
	m_pMainWnd = &dlg;
	int nResponse = dlg.DoModal();
	if (nResponse == IDOK)
	{
		// TODO: ダイアログが <OK> で消された時のコードを
		//       記述してください。
	}
	else if (nResponse == IDCANCEL)
	{
		// TODO: ダイアログが <ｷｬﾝｾﾙ> で消された時のコードを
		//       記述してください。
	}

	// ダイアログが閉じられてからアプリケーションのメッセージ ポンプを開始するよりは、
	// アプリケーションを終了するために FALSE を返してください。
	return FALSE;
}
