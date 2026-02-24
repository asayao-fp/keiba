#pragma once
#include "afxcmn.h"
#include "afxwin.h"
#include "jvlink1.h"


// dlgDel ダイアログ

class dlgDel : public CDialog
{
	DECLARE_DYNAMIC(dlgDel)

public:
	dlgDel(CWnd* pParent = NULL);   // 標準コンストラクタ
	virtual ~dlgDel();

// ダイアログ データ
	enum { IDD = IDD_DIALOG1 };

protected:
	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV サポート

	DECLARE_MESSAGE_MAP()
						

private:

	// ファイル削除
	CString m_txtDel;

	//OKボタンクリック
	afx_msg  void dlgDel::OnBnClickedOk();
	



};
