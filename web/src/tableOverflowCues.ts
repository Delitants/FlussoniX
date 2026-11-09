export function watchTableOverflow(root:Element):()=>void{
 const observed=new Set<Element>();
 let frame=0;
 const resize=new ResizeObserver(()=>schedule());
 function update(){
  frame=0;
  const current=new Set<Element>();
  root.querySelectorAll<HTMLElement>('.table-wrap').forEach(wrap=>{
   const table=wrap.querySelector('table');
   if(!table)return;
   current.add(wrap);current.add(table);
   for(const element of [wrap,table])if(!observed.has(element)){resize.observe(element);observed.add(element)}
   wrap.classList.toggle('has-table-overflow',table.scrollWidth>wrap.clientWidth+1);
  });
  for(const element of observed)if(!current.has(element)){resize.unobserve(element);observed.delete(element)}
 }
 function schedule(){if(!frame)frame=requestAnimationFrame(update)}
 const mutations=new MutationObserver(schedule);
 mutations.observe(root,{childList:true,subtree:true,characterData:true});
 schedule();
 return ()=>{mutations.disconnect();resize.disconnect();if(frame)cancelAnimationFrame(frame)};
}
