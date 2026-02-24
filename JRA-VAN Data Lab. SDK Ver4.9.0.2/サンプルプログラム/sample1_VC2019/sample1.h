// sample1.h : SAMPLE1 アプリケーションのメイン ヘッダー ファイルです。
//

#if !defined(AFX_SAMPLE1_H__CDA3D124_7B34_11D7_916F_0003479BEB3F__INCLUDED_)
#define AFX_SAMPLE1_H__CDA3D124_7B34_11D7_916F_0003479BEB3F__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#ifndef __AFXWIN_H__
	#error include 'stdafx.h' before including this file for PCH
#endif

#include "resource.h"		// メイン シンボル

//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(sample1.h)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

class CSample1App : public CWinApp
{
public:
	CSample1App();

// オーバーライド
	// ClassWizard は仮想関数のオーバーライドを生成します。
	//{{AFX_VIRTUAL(CSample1App)
	public:
	virtual BOOL InitInstance();
	//}}AFX_VIRTUAL

// インプリメンテーション

	//{{AFX_MSG(CSample1App)
		// メモ - ClassWizard はこの位置にメンバ関数を追加または削除します。
		//        この位置に生成されるコードを編集しないでください。
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
};


/////////////////////////////////////////////////////////////////////////////

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ は前行の直前に追加の宣言を挿入します。

#endif // !defined(AFX_SAMPLE1_H__CDA3D124_7B34_11D7_916F_0003479BEB3F__INCLUDED_)
