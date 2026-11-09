import React,{useLayoutEffect,useRef} from 'react';
import {createPortal} from 'react-dom';

type Props={children:React.ReactNode;labelledBy:string;onClose:()=>void;className?:string};

const controls='a[href],button,input,select,textarea,summary,[tabindex]:not([tabindex="-1"])';
const modalStack:HTMLElement[]=[];
let originalRootInert=false;

export function Modal({children,labelledBy,onClose,className=''}:Props){
 const dialog=useRef<HTMLElement>(null);
 const close=useRef(onClose);
 close.current=onClose;

 useLayoutEffect(()=>{
  const element=dialog.current;
  const root=document.getElementById('root');
  if(!element||!root)return;
  const opener=document.activeElement instanceof HTMLElement?document.activeElement:null;
  if(modalStack.length===0){originalRootInert=root.inert;root.inert=true;}
  modalStack.push(element);
  const isTop=()=>modalStack.at(-1)===element;
  const available=()=>Array.from(element.querySelectorAll<HTMLElement>(controls)).filter(control=>control.tabIndex>=0&&!control.matches(':disabled')&&control.getClientRects().length>0);
  const onKeyDown=(event:KeyboardEvent)=>{
   if(!isTop())return;
   if(event.key==='Escape'&&!event.defaultPrevented){event.preventDefault();close.current();return;}
   if(event.key!=='Tab')return;
   const items=available();
   const first=items[0],last=items.at(-1);
   if(!first){event.preventDefault();element.focus({preventScroll:true});return;}
   const active=document.activeElement;
   if(!element.contains(active)||active===element){event.preventDefault();(event.shiftKey?last:first)?.focus();return;}
   if(event.shiftKey&&active===first){event.preventDefault();last?.focus();}
   else if(!event.shiftKey&&active===last){event.preventDefault();first.focus();}
  };
  const onFocusIn=(event:FocusEvent)=>{
   if(isTop()&&!element.contains(event.target as Node))element.focus({preventScroll:true});
  };
  document.addEventListener('keydown',onKeyDown);
  document.addEventListener('focusin',onFocusIn);
  element.focus({preventScroll:true});
  return()=>{
   document.removeEventListener('keydown',onKeyDown);
   document.removeEventListener('focusin',onFocusIn);
   const wasTop=isTop();
   const index=modalStack.indexOf(element);
   if(index!==-1)modalStack.splice(index,1);
   const previous=wasTop?modalStack.at(-1):undefined;
   if(modalStack.length===0)root.inert=originalRootInert;
   queueMicrotask(()=>{
    if(previous){
     if(modalStack.at(-1)===previous&&previous.isConnected)previous.focus({preventScroll:true});
    }else if(wasTop&&modalStack.length===0&&opener?.isConnected){
     opener.focus({preventScroll:true});
    }
   });
  };
 },[]);

 return createPortal(<div className="modal-backdrop"><section ref={dialog} className={'modal'+(className?' '+className:'')} role="dialog" aria-modal="true" aria-labelledby={labelledBy} tabIndex={-1}>{children}</section></div>,document.body);
}
