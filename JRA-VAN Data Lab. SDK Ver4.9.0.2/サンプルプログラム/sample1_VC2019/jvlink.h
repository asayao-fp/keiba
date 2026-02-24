// jvlink.h  : Microsoft Visual C++ で作成された ActiveX コントロール ラッパー クラスの宣言です。

#pragma once

/////////////////////////////////////////////////////////////////////////////
// CJVLink

class CJVLink : public CWnd
{
protected:
	DECLARE_DYNCREATE(CJVLink)
public:
	CLSID const& GetClsid()
	{
		static CLSID const clsid
			= { 0x2AB1774D, 0xC41, 0x11D7, { 0x91, 0x6F, 0x0, 0x3, 0x47, 0x9B, 0xEB, 0x3F } };
		return clsid;
	}
	virtual BOOL Create(LPCTSTR lpszClassName, LPCTSTR lpszWindowName, DWORD dwStyle,
						const RECT& rect, CWnd* pParentWnd, UINT nID, 
						CCreateContext* pContext = NULL)
	{ 
		return CreateControl(GetClsid(), lpszWindowName, dwStyle, rect, pParentWnd, nID); 
	}

    BOOL Create(LPCTSTR lpszWindowName, DWORD dwStyle, const RECT& rect, CWnd* pParentWnd, 
				UINT nID, CFile* pPersist = NULL, BOOL bStorage = FALSE,
				BSTR bstrLicKey = NULL)
	{ 
		return CreateControl(GetClsid(), lpszWindowName, dwStyle, rect, pParentWnd, nID,
		pPersist, bStorage, bstrLicKey); 
	}

// 属性
public:

// 操作
public:

	long JVSetSavePath(LPCTSTR savepath)
	{
		long result;
		static BYTE parms[] = VTS_BSTR ;
		InvokeHelper(0x1, DISPATCH_METHOD, VT_I4, (void*)&result, parms, savepath);
		return result;
	}
	CString get_m_savepath()
	{
		CString result;
		InvokeHelper(0x2, DISPATCH_PROPERTYGET, VT_BSTR, (void*)&result, NULL);
		return result;
	}
	CString get_m_servicekey()
	{
		CString result;
		InvokeHelper(0x3, DISPATCH_PROPERTYGET, VT_BSTR, (void*)&result, NULL);
		return result;
	}
	long JVInit(LPCTSTR sid)
	{
		long result;
		static BYTE parms[] = VTS_BSTR ;
		InvokeHelper(0x4, DISPATCH_METHOD, VT_I4, (void*)&result, parms, sid);
		return result;
	}
	long JVClose()
	{
		long result;
		InvokeHelper(0x5, DISPATCH_METHOD, VT_I4, (void*)&result, NULL);
		return result;
	}
	long JVSetUIProperties()
	{
		long result;
		InvokeHelper(0x6, DISPATCH_METHOD, VT_I4, (void*)&result, NULL);
		return result;
	}
	long JVOpen(LPCTSTR dataspec, LPCTSTR fromdate, long option, long * readcount, long * downloadcount, BSTR * lastfiletimestamp)
	{
		long result;
		static BYTE parms[] = VTS_BSTR VTS_BSTR VTS_I4 VTS_PI4 VTS_PI4 VTS_PBSTR ;
		InvokeHelper(0x7, DISPATCH_METHOD, VT_I4, (void*)&result, parms, dataspec, fromdate, option, readcount, downloadcount, lastfiletimestamp);
		return result;
	}
	long JVStatus()
	{
		long result;
		InvokeHelper(0x8, DISPATCH_METHOD, VT_I4, (void*)&result, NULL);
		return result;
	}
	long JVRead(BSTR * buff, long * size, BSTR * filename)
	{
		long result;
		static BYTE parms[] = VTS_PBSTR VTS_PI4 VTS_PBSTR ;
		InvokeHelper(0x9, DISPATCH_METHOD, VT_I4, (void*)&result, parms, buff, size, filename);
		return result;
	}
	long CJVLink::JVGets(VARIANT* buff, long size, BSTR* filename)
	{
		long result;
		static BYTE parms[] =
			VTS_PVARIANT VTS_I4 VTS_PBSTR;
		InvokeHelper(0x16, DISPATCH_METHOD, VT_I4, (void*)&result, parms,
			buff, size, filename);
		return result;
	}
	long JVRTOpen(LPCTSTR dataspec, LPCTSTR key)
	{
		long result;
		static BYTE parms[] = VTS_BSTR VTS_BSTR ;
		InvokeHelper(0xa, DISPATCH_METHOD, VT_I4, (void*)&result, parms, dataspec, key);
		return result;
	}
	void JVCancel()
	{
		InvokeHelper(0xb, DISPATCH_METHOD, VT_EMPTY, NULL, NULL);
	}
	long JVFiledelete(LPCTSTR filename)
	{
		long result;
		static BYTE parms[] = VTS_BSTR ;
		InvokeHelper(0xc, DISPATCH_METHOD, VT_I4, (void*)&result, parms, filename);
		return result;
	}
	long JVSetServiceKey(LPCTSTR servicekey)
	{
		long result;
		static BYTE parms[] = VTS_BSTR ;
		InvokeHelper(0xd, DISPATCH_METHOD, VT_I4, (void*)&result, parms, servicekey);
		return result;
	}
	long get_m_saveflag()
	{
		long result;
		InvokeHelper(0xe, DISPATCH_PROPERTYGET, VT_I4, (void*)&result, NULL);
		return result;
	}
	long JVSetSaveFlag(long saveflag)
	{
		long result;
		static BYTE parms[] = VTS_I4 ;
		InvokeHelper(0xf, DISPATCH_METHOD, VT_I4, (void*)&result, parms, saveflag);
		return result;
	}
	CString get_m_JVLinkVersion()
	{
		CString result;
		InvokeHelper(0x10, DISPATCH_PROPERTYGET, VT_BSTR, (void*)&result, NULL);
		return result;
	}
	long get_m_TotalReadFilesize()
	{
		long result;
		InvokeHelper(0x11, DISPATCH_PROPERTYGET, VT_I4, (void*)&result, NULL);
		return result;
	}
	long get_m_CurrentReadFilesize()
	{
		long result;
		InvokeHelper(0x12, DISPATCH_PROPERTYGET, VT_I4, (void*)&result, NULL);
		return result;
	}
	void CJVLink::SetParentHWnd(long nNewValue)
	{
		static BYTE parms[] =
			VTS_I4;
		InvokeHelper(0x15, DISPATCH_PROPERTYPUT, VT_EMPTY, NULL, parms,
			 nNewValue);
	}
	void JVSkip()
	{
		InvokeHelper(0x13, DISPATCH_METHOD, VT_EMPTY, NULL, NULL);
	}
	long JVMVPlay(LPCTSTR key)
	{
		long result;
		static BYTE parms[] = VTS_BSTR ;
		InvokeHelper(0x17, DISPATCH_METHOD, VT_I4, (void*)&result, parms, key);
		return result;
	}
	long JVMVCheck(LPCTSTR key)
	{
		long result;
		static BYTE parms[] = VTS_BSTR ;
		InvokeHelper(0x18, DISPATCH_METHOD, VT_I4, (void*)&result, parms, key);
		return result;
	}
	long JVFukuFile(LPCTSTR pattern, LPCTSTR filepath)
	{
		long result;
		static BYTE parms[] = VTS_BSTR VTS_BSTR ;
		InvokeHelper(0x19, DISPATCH_METHOD, VT_I4, (void*)&result, parms, pattern, filepath);
		return result;
	}
	long JVFuku(LPCTSTR pattern, VARIANT * buff)
	{
		long result;
		static BYTE parms[] = VTS_BSTR VTS_PVARIANT ;
		InvokeHelper(0x1a, DISPATCH_METHOD, VT_I4, (void*)&result, parms, pattern, buff);
		return result;
	}
};
